package model

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/iox"
)

// TODO(herohde) 8/23/2024: Dispatcher/Processor/Range are all somewhat prescriptive. We may want to
// add options or alternatives: for example, non-standard grant state lookup, no connection pool, and
// range creation on allocated may be desirable in some cases. Punt for now.

const maxMessageSize = 64 * 1024 * 1024 // 64mb

// DispatchFilter is a hook for grants, used by the Dispatcher. A list of filters is a chain. The first
// filter in the chain that accepts a grant handles it.
type DispatchFilter interface {
	// Init is called after the Dispatcher joins the work distribution process.
	Init(service QualifiedServiceName, pool ConnectionPool)
	// TryHandle is called to find a filter for a grant. Return true if accepted. If not accepted it must
	// return immediately. Blocking.
	TryHandle(ctx context.Context, id GrantID, shard Shard, ownership Ownership) bool
}

// DispatcherConnectionPoolFn is a function for creating the dispatcher connection pool.
type DispatcherConnectionPoolFn func(id Instance, clusters <-chan Cluster) ConnectionPool

// DispatcherClusterFn is a cluster chan transformation function. Used to intercept and
// possibly modify the Cluster updates from the server. Applied before the connection pool
// is created.
type DispatcherClusterFn func(clusters <-chan Cluster) <-chan Cluster

type dispatcherOptions struct {
	opts []ConsumerOption
	fn   DispatcherClusterFn
	pool DispatcherConnectionPoolFn
}

// DispatcherOption provides advanced options to Dispatcher.
type DispatcherOption func(option *dispatcherOptions)

func WithDispatcherConsumerOptions(opts ...ConsumerOption) DispatcherOption {
	return func(o *dispatcherOptions) {
		o.opts = append(o.opts, opts...)
	}
}

func WithDispatcherClusterFn(fn DispatcherClusterFn) DispatcherOption {
	return func(o *dispatcherOptions) {
		o.fn = fn
	}
}

func WithDispatcherConnectionPoolFn(fn DispatcherConnectionPoolFn) DispatcherOption {
	return func(o *dispatcherOptions) {
		o.pool = fn
	}
}

// Dispatcher joins the work distribution process for a service, maintaining a connection pool. Each received
// grant is passed to a filter chain and given to the first DispatchFilter that accepts it. The Dispatcher
// de-registers on context cancellation (or Drain) and is closed on its completion.
type Dispatcher struct {
	ConnectionPool
	cl      clock.Clock
	id      Instance
	service QualifiedServiceName
	chain   []DispatchFilter

	initialized, drain, closed iox.AsyncCloser
}

func NewDispatcher(ctx context.Context, cl clock.Clock, client ConsumerClient, loc location.Location, endpoint string, service QualifiedServiceName, chain []DispatchFilter, opts ...ConsumerOption) *Dispatcher {
	return NewDispatcherEx(ctx, cl, client, loc, endpoint, service, chain, WithDispatcherConsumerOptions(opts...))
}

func NewDispatcherEx(ctx context.Context, cl clock.Clock, client ConsumerClient, loc location.Location, endpoint string, service QualifiedServiceName, chain []DispatchFilter, opts ...DispatcherOption) *Dispatcher {
	ret := &Dispatcher{
		cl:          cl,
		id:          NewInstance(location.NewInstance(loc), endpoint),
		service:     service,
		chain:       chain,
		initialized: iox.NewAsyncCloser(),
		drain:       iox.NewAsyncCloser(),
	}

	var options dispatcherOptions
	for _, fn := range opts {
		fn(&options)
	}

	wctx, _ := contextx.WithQuitCancel(ctx, ret.drain.Closed())                             // drain => stop splitter
	clusters, closed := client.Join(wctx, ret.id, ret.service, ret.handle, options.opts...) // closed == all grants are relinquished
	ret.closed = iox.WithQuit(closed.Closed(), iox.NewAsyncCloser())

	if options.fn != nil {
		clusters = options.fn(clusters)
	}
	if options.pool != nil {
		ret.ConnectionPool = options.pool(ret.id, clusters)
	} else {
		ret.ConnectionPool = NewConnectionPool(ctx, cl, ret.id.ID(), clusters, grpcx.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)))
	}

	for _, h := range ret.chain {
		h.Init(ret.service, ret)
	}
	ret.initialized.Close()
	return ret

}

func (d *Dispatcher) handle(ctx context.Context, grant GrantID, shard Shard, lease Ownership) {
	<-d.initialized.Closed()

	now := d.cl.Now()
	log.Debugf(ctx, "Received grant %v:%v", grant, shard)

	if contextx.IsCancelled(ctx) || lease.Expired().IsClosed() {
		log.Errorf(ctx, "Unexpected: grant %v:%v expired before dispatch, expiration=%v", grant, shard, lease.Expiration())
		return
	}
	for _, h := range d.chain {
		if h.TryHandle(ctx, grant, shard, lease) {
			log.Debugf(ctx, "Relinquished grant %v:%v after %v, expired=%v", grant, shard, d.cl.Since(now), lease.Expired().IsClosed())
			return
		}
	}

	log.Errorf(ctx, "Unexpected: grant %v:%v has no matching handler. Relinquishing", grant, shard)
}

// ID returns the client consumer instance.
func (d *Dispatcher) ID() Instance {
	return d.id
}

// Service returns the qualified service name used to join.
func (d *Dispatcher) Service() QualifiedServiceName {
	return d.service
}

func (d *Dispatcher) Drain(timeout time.Duration) {
	d.drain.Close()
	d.cl.AfterFunc(timeout, d.closed.Close)
}

func (d *Dispatcher) IsClosed() bool {
	return d.closed.IsClosed()
}

func (d *Dispatcher) Closed() <-chan struct{} {
	return d.closed.Closed()
}

func (d *Dispatcher) String() string {
	return fmt.Sprintf("%v/%v", d.service, d.id.ID())
}

type RangeFactory[V Range] func(ctx context.Context, grant GrantID, shard Shard, ownership Ownership) V
type RangeFactoryEx[V Range] func(shard Shard) (RangeFactory[V], bool)

// Range is a lifecycle interface to participate in graceful Grant state transitions, used by a
// Processor. A Range is created when the Grant counterpart is UNLOADED and closed when the Grant
// expired or is being relinquished. It may close on its own initiative, which forces a relinquish.
type Range interface {
	iox.AsyncCloser

	// Initialized returns a closer for when initialized. Must be closed!
	Initialized() iox.RAsyncCloser
	// Drain is called when a Grant is revoked. It returns a closer for when draining in complete.
	Drain(ctx context.Context, timeout time.Duration) iox.RAsyncCloser
}

// Processor manages Ranges for one or more domains on single node, acting as a Proxy. A Range is created for
// each given grant. The Processor handles the interaction between Grant states and Range.
type Processor[T, K any, V Range] struct {
	cl      clock.Clock
	service QualifiedServiceName
	rfn     RemoteFn[T]
	fn      func(K) ServiceDomainKey
	factory RangeFactoryEx[V]

	grants *GrantMap[V]

	pool        ConnectionPool
	resolver    Resolver[T, K]
	initialized iox.AsyncCloser
}

// NewProcessor creates a Processor for a single domain.
func NewProcessor[T, K any, V Range](cl clock.Clock, domain DomainName, rfn RemoteFn[T], fn func(K) DomainKey, factory RangeFactory[V]) *Processor[T, K, V] {
	sfn := func(k K) ServiceDomainKey {
		return ServiceDomainKey{Domain: domain, Key: fn(k)}
	}
	return NewProcessorEx(cl, rfn, sfn, func(shard Shard) (RangeFactory[V], bool) {
		return factory, shard.Domain.Domain == domain
	})
}

// NewProcessorEx creates a Processor for multiple, dynamically determined domains.
func NewProcessorEx[T, K any, V Range](cl clock.Clock, rfn RemoteFn[T], fn func(K) ServiceDomainKey, factory RangeFactoryEx[V]) *Processor[T, K, V] {
	return &Processor[T, K, V]{
		cl:          cl,
		rfn:         rfn,
		fn:          fn,
		factory:     factory,
		grants:      NewGrantMap[V](),
		initialized: iox.NewAsyncCloser(),
	}
}

func (p *Processor[T, K, V]) Init(service QualifiedServiceName, pool ConnectionPool) {
	p.service = service
	p.pool = pool
	p.resolver = NewDomainResolver(pool, p.rfn, p.DomainKey)
	p.initialized.Close()
}

func (p *Processor[T, K, V]) TryHandle(ctx context.Context, grant GrantID, shard Shard, lease Ownership) bool {
	<-p.initialized.Closed()

	fn, ok := p.factory(shard)
	if !ok {
		return false // skip: not handling domain
	}
	p.handle(ctx, fn, grant, shard, lease)
	return true
}

func (p *Processor[T, K, V]) handle(ctx context.Context, fn RangeFactory[V], grant GrantID, shard Shard, lease Ownership) {
	// (1) Allocated only. Wait for counterpart to unload, which usually means that it does not update
	// the underlying state anymore. It is thereafter safe to initialize.

	loader, err := WaitForUnload(ctx, lease)
	if err != nil {
		log.Errorf(ctx, "Grant %v:%v counterpart failed to unload: %v", grant, shard, err)
		return
	}

	// (2) Create range and wait for initialization. Then signal loaded. Loaded is an owning state.
	// We delay Range creation to after ALLOCATED_UNLOADED to simplify initialization.

	r := fn(ctx, grant, shard, lease)
	defer r.Close()

	p.grants.Allocated(grant, shard, r)
	defer p.grants.Delete(grant, shard)

	wctx, _ := contextx.WithQuitCancel(ctx, r.Closed())
	if err := WaitForAction(wctx, r.Initialized(), lease); err != nil {
		log.Errorf(ctx, "Grant %v:%v failed to initialize: %v", grant, shard, err)
		return
	}
	loader.Load()

	p.grants.Loaded(grant, shard, r)

	// (3) Wait for activation. That indicates that the counterparty is gone.

	if err := WaitForActive(wctx, lease); err != nil {
		log.Infof(ctx, "Grant %v:%v failed to activate: %v", grant, shard, err)
		return
	}
	p.grants.Activate(grant, shard, r)

	log.Infof(ctx, "Grant %v:%v activated", grant, shard)

	// (4) Steady state with exclusive ownership. Wait for revoke, loss of ownership or range
	// closure. If revoked, drain with remaining lease timeout.

	unloader, err := WaitForRevoke(wctx, lease)
	if err != nil {
		log.Errorf(ctx, "Grant %v:%v expired or range closed, lease=%v: %v", grant, shard, lease.Expiration().Sub(p.cl.Now()), err)
		return
	}
	p.grants.Revoke(grant, shard, r)

	timeout := lease.Expiration().Sub(p.cl.Now())
	log.Infof(ctx, "Grant %v:%v revoked, lease=%v.", grant, shard, timeout)

	unloaded := r.Drain(ctx, timeout)

	// (5) Wait for range drain completion. Signal unloaded and wait for counterpart to load. This handshake
	// avoids an unavailability gap when shards are moved.

	if err := WaitForAction(ctx /* not wctx */, unloaded, lease); err != nil {
		log.Errorf(ctx, "Grant %v:%v failed to drain: %v", grant, shard, err)
		return
	}
	if r.IsClosed() {
		return // relinquish: drain closed the range. Return avoids error log from WaitForLoad.
	}
	unloader.Unload()

	p.grants.Unloaded(grant, shard, r)

	if err := WaitForLoad(wctx, lease); err != nil {
		log.Warnf(ctx, "Grant %v:%v expired or closed before counterpart load, lease=%v: %v", grant, shard, timeout, err)
		return
	}
}

func (p *Processor[T, K, V]) Lookup(key K, grants ...GrantState) (V, bool) {
	<-p.initialized.Closed()
	return p.grants.Lookup(p.DomainKey(key), grants...)
}

func (p *Processor[T, K, V]) Cluster() (Cluster, bool) {
	<-p.initialized.Closed()
	return p.pool.Cluster()
}

func (p *Processor[T, K, V]) Resolve(ctx context.Context, key K) (T, error) {
	<-p.initialized.Closed()
	return p.resolver.Resolve(ctx, key)
}

func (p *Processor[T, K, V]) DomainKey(key K) QualifiedDomainKey {
	sdk := p.fn(key)
	return QualifiedDomainKey{Domain: QualifiedDomainName{Service: p.service, Domain: sdk.Domain}, Key: sdk.Key}
}

func (p *Processor[T, K, V]) Location(key K) (location.Location, bool) {
	return p.resolver.Location(key)
}
