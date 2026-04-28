package model

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	"go.atoms.co/iox"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/slicex"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	splitterpb "go.atoms.co/splitter/pb"
)

var (
	ErrRevoked = errors.New("grant revoked")
	ErrExpired = errors.New("grant expired")

	// ClientVersion of the client library.
	ClientVersion = "1.1.0"
)

// Ownership holds information about the grant state and expiration, as well as signals for
// participating in graceful transitions via load/unload. Ownership is meant to be used via
// the WaitFor helpers to ensure correct progression through the grant lifecycle.
//
// Grant handlers that use graceful handover follow this sequence:
//
//	func handler(o Ownership) {
//	  unloader := WaitForUnload  // wait for prior counterparty to be UNLOADED
//	  unloader.Load()            // ALLOCATED -> LOADED
//	  .. preferred part owner ..
//	  WaitForActive              // LOADED    -> ACTIVE
//	  .. exclusive owner ..
//	  loader := WaitForRevoke    // ACTIVE    -> REVOKED
//	  loader.Unload()            // REVOKED   -> UNLOADED
//	  .. still part owner but not preferred ..
//	  WaitForLoad                // wait for next counterparty to be LOADED
//	}                            // returning relinquishes the grant
//
// The load/unload aspects are optional. If not needed, the handler can be:
//
//	func handler(o Ownership) {
//	  WaitForActive              // ALLOCATED -> ACTIVE
//	  .. exclusive owner ..
//	  WaitForRevoke              // ACTIVE    -> REVOKED
//	  .. still owner ..
//	}                            // returning relinquishes the grant
//
// The WaitFor helpers ensure that if a step does not happen, such as a grant directly being
// assigned in ACTIVE state, the logic progresses as expected. If the grant is unexpectedly lost,
// the helpers return an error to let the handler exit.
type Ownership interface {
	// Active returns a quit channel to signal that the Grant has been activated.
	Active() iox.RAsyncCloser
	// RequestRevoke sends a request to initiate revoke process. The grant won't be immediately
	// revoked, the caller should explicitly release the grant after some time after calling this method,
	// regardless of the revoke message being sent successfully or not.
	RequestRevoke()
	// Revoked returns a quit channel to signal that the Grant has been revoked.
	Revoked() iox.RAsyncCloser
	// Expired returns a quit channel to signal that the Grant lease has expired.
	Expired() iox.RAsyncCloser

	// Loader returns controls for the loading phase.
	Loader() Loader
	// Unloader returns controls for the unloading phase.
	Unloader() Unloader

	// Expiration returns the expiration time of the lease. The expiration is updated periodically by the server under
	// normal circumstances. When the expiration time is past the current moment the grant is no longer assigned
	// to the current consumer and the consumer must abort processing immediately.
	Expiration() time.Time
}

// Loader is used during the loading phase of a grant to coordinate transfer of ownership. It
// holds information about whether the prior counterpart grant (if any) has been unloaded and can signal
// when its loading is complete, which will make it the target for resolution.
type Loader interface {
	// Unloaded returns a closer for when the counterpart transitions to Unloaded. It may never happen.
	Unloaded() iox.RAsyncCloser
	// Load transitions the grant from Allocated to Loaded. Has no effect if the grant is active.
	Load()
}

// Unloader is used during the unloading (or draining) phase of a grant to coordinate transfer of
// ownership. It can signal when unloading is complete and holds information about whether the next
// counterpart grant (if any) has been loaded, after which handover can complete.
type Unloader interface {
	// Unload transitions the grant from Revoked to Unloaded.
	Unload()
	// Loaded returns a closer for when the counterpart transitions to Loaded, usually in response to
	// the Unload signal. It may never happen.
	Loaded() iox.RAsyncCloser
}

// WaitForUnload blocks on prior counterpart Grant unloading. Typically, this signals that the handler
// can safely initialize and become ready to assume ownership. The loader is returned to signal
// initialization completion. Returns an error if the grant is revoked or expires before then. Cancellable.
func WaitForUnload(ctx context.Context, o Ownership) (Loader, error) {
	loader := o.Loader()

	select {
	case <-o.Active().Closed():
		return loader, nil // Consider unloaded if shard is activated before unload
	case <-loader.Unloaded().Closed():
		return loader, nil
	case <-o.Revoked().Closed():
		return nil, ErrRevoked
	case <-o.Expired().Closed():
		return nil, ErrExpired
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WaitForActive blocks on Grant activation. Returns an error if the grant is revoked or expires before
// then. Cancellable.
func WaitForActive(ctx context.Context, o Ownership) error {
	select {
	case <-o.Active().Closed():
		return nil
	case <-o.Revoked().Closed():
		return ErrRevoked
	case <-o.Expired().Closed():
		return ErrExpired
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForRevoke blocks on Grant revocation. Returns an error if the grant expires before then. Cancellable.
// Does not error on grant activation, since activation does not preclude revocation.
func WaitForRevoke(ctx context.Context, o Ownership) (Unloader, error) {
	select {
	case <-o.Revoked().Closed():
		return o.Unloader(), nil
	case <-o.Expired().Closed():
		return nil, ErrExpired
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WaitForLoad blocks on next counterpart Grant loading. Returns an error if the grant expires before then.
// Cancellable.
func WaitForLoad(ctx context.Context, o Ownership) error {
	select {
	case <-o.Unloader().Loaded().Closed():
		return nil
	case <-o.Expired().Closed():
		return ErrExpired
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForAction blocks on a user action (e.g., initialization or drain), via a given async closure.
// Returns an error if the grant expires before then. Cancellable.
func WaitForAction(ctx context.Context, c iox.RAsyncCloser, o Ownership) error {
	select {
	case <-c.Closed():
		return nil
	case <-o.Expired().Closed():
		return ErrExpired
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UpdateTenantOption represents an option to NewTenant.
type UpdateTenantOption func(*splitterpb.UpdateTenantRequest)

// WithUpdateTenantOperational update the operational metadata of a tenant.
func WithUpdateTenantOperational(operational TenantOperational) UpdateTenantOption {
	return func(request *splitterpb.UpdateTenantRequest) {
		request.Operational = UnwrapTenantOperational(operational)
	}
}

// WithUpdateTenantConfig defines config for a tenant.
func WithUpdateTenantConfig(config TenantConfig) UpdateTenantOption {
	return func(request *splitterpb.UpdateTenantRequest) {
		request.Config = UnwrapTenantConfig(config)
	}
}

// UpdateServiceOption represents an option to NewService.
type UpdateServiceOption func(*splitterpb.UpdateServiceRequest)

// WithUpdateServiceOperational update the operational metadata of a service.
func WithUpdateServiceOperational(operational ServiceOperational) UpdateServiceOption {
	return func(request *splitterpb.UpdateServiceRequest) {
		request.Operational = UnwrapServiceOperational(operational)
	}
}

// WithUpdateServiceConfig defines config for a tenant.
func WithUpdateServiceConfig(config ServiceConfig) UpdateServiceOption {
	return func(request *splitterpb.UpdateServiceRequest) {
		request.Config = UnwrapServiceConfig(config)
	}
}

// NewDomainOption represents an option to NewDomain.
type NewDomainOption func(*splitterpb.NewDomainRequest)

// WithNewDomainState updates the state of a domain.
func WithNewDomainState(state DomainState) NewDomainOption {
	return func(request *splitterpb.NewDomainRequest) {
		request.State = state
	}
}

// UpdateDomainOption represents an option to UpdateDomain.
type UpdateDomainOption func(*splitterpb.UpdateDomainRequest)

// WithUpdateDomainOperational update the operational metadata of a domain.
func WithUpdateDomainOperational(operational DomainOperational) UpdateDomainOption {
	return func(request *splitterpb.UpdateDomainRequest) {
		request.Operational = UnwrapDomainOperational(operational)
	}
}

// WithUpdateDomainState update the state of a domain.
func WithUpdateDomainState(state DomainState) UpdateDomainOption {
	return func(request *splitterpb.UpdateDomainRequest) {
		request.State = state
	}
}

// WithUpdateDomainConfig defines config for a domain.
func WithUpdateDomainConfig(config DomainConfig) UpdateDomainOption {
	return func(request *splitterpb.UpdateDomainRequest) {
		request.Config = UnwrapDomainConfig(config)
	}
}

// Handler processes grants. Must be concurrency-safe.
// The implementation should use ownership to detect changes in grant state and act accordingly.
// The context is cancelled when the grant is terminated and its processing should stop immediately. For graceful
// shutdown, the handler must act on grant revoke (which is followed by context cancellation shortly after).
type Handler func(ctx context.Context, id GrantID, shard Shard, ownership Ownership)

// Client is a client for interacting with Splitter.
type Client interface {
	ListTenants(ctx context.Context) ([]TenantInfo, error)
	NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (TenantInfo, error)
	InfoTenant(ctx context.Context, name TenantName) (TenantInfo, error)
	UpdateTenant(ctx context.Context, name TenantName, guard Version, opts ...UpdateTenantOption) (TenantInfo, error)
	DeleteTenant(ctx context.Context, name TenantName) error

	ListServices(ctx context.Context, tenant TenantName) ([]ServiceInfoEx, error)
	NewService(ctx context.Context, name QualifiedServiceName, cfg ServiceConfig) (ServiceInfo, error)
	InfoService(ctx context.Context, name QualifiedServiceName) (ServiceInfoEx, error)
	UpdateService(ctx context.Context, name QualifiedServiceName, guard Version, opts ...UpdateServiceOption) (ServiceInfo, error)
	DeleteService(ctx context.Context, name QualifiedServiceName) error

	ListDomains(ctx context.Context, service QualifiedServiceName) ([]Domain, error)
	NewDomain(ctx context.Context, name QualifiedDomainName, domainType DomainType, cfg DomainConfig, opts ...NewDomainOption) (Domain, error)
	UpdateDomain(ctx context.Context, name QualifiedDomainName, guard Version, opts ...UpdateDomainOption) (Domain, error)
	DeleteDomain(ctx context.Context, name QualifiedDomainName) error

	ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error)
	InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error)
}

type ConsumerOption func(opts Options)

func WithKeyNames(names ...DomainKeyName) ConsumerOption {
	return func(opts Options) {
		opts.pb.Names = slicex.Map(names, DomainKeyName.ToProto)
	}
}
func WithCapacityLimit(limit int) ConsumerOption {
	return func(opts Options) {
		opts.pb.CapacityLimit = uint64(limit)
	}
}

type ConsumerClient interface {
	// Join adds the consumer to the work distribution process. During this process the consumer receives
	// assigned grants and, separately, grants assigned to all consumers.
	// Non-blocking.
	// Returns a channel with clusters and a closer to signal the consumer has closed
	Join(ctx context.Context, consumer Consumer, service QualifiedServiceName, handler Handler, opts ...ConsumerOption) (<-chan Cluster, iox.RAsyncCloser)
}

type consumerClient struct {
	consumer splitterpb.ConsumerServiceClient
}

func (c consumerClient) Join(ctx context.Context, consumer Consumer, service QualifiedServiceName, handler Handler, opts ...ConsumerOption) (<-chan Cluster, iox.RAsyncCloser) {
	quit := iox.NewAsyncCloser()

	log.Infof(ctx, "Starting consumer %v to service %v using client with version %v", consumer, service, ClientVersion)

	joinFn := func(ctx context.Context, self location.Instance, handler grpcx.Handler[ConsumerMessage, ConsumerMessage]) error {
		sess, establish, out := session.NewClient(ctx, self)
		defer sess.Close()
		wctx, _ := contextx.WithQuitCancel(ctx, sess.Closed()) // cancel context if session client closes

		return grpcx.Connect(wctx, c.consumer.Join, func(ctx context.Context, in <-chan *splitterpb.JoinMessage) (<-chan *splitterpb.JoinMessage, error) {
			ch := chanx.MapIf(in, func(pb *splitterpb.JoinMessage) (ConsumerMessage, bool) {
				if pb.GetSession() != nil {
					sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session client
					return ConsumerMessage{}, false
				}
				return WrapConsumerMessage(pb.GetConsumer()), true
			})

			resp, err := handler(ctx, ch)
			if err != nil {
				return nil, ToGRPCError(err)
			}

			joined := session.Connect(sess, establish, chanx.Map(resp, NewJoinMessage), out, NewJoinSessionMessage)
			return chanx.Map(joined, UnwrapJoinMessage), nil
		})
	}
	pool, clusters := NewWorkPool(consumer, service, nil, joinFn, handler, opts...)

	go func() {
		defer quit.Close()
		<-ctx.Done()

		pool.Drain(1 * time.Minute)
		now := time.Now()
		<-pool.Closed()
		log.Infof(ctx, "Closed work pool in %v", time.Since(now))
	}()

	return clusters, quit
}

func NewConsumerClient(cc *grpc.ClientConn) ConsumerClient {
	return &consumerClient{
		consumer: splitterpb.NewConsumerServiceClient(cc),
	}
}

type client struct {
	consumer   splitterpb.ConsumerServiceClient
	management splitterpb.ManagementServiceClient
	placement  splitterpb.PlacementServiceClient
}

func NewClient(cc *grpc.ClientConn) Client {
	return &client{
		consumer:   splitterpb.NewConsumerServiceClient(cc),
		management: splitterpb.NewManagementServiceClient(cc),
		placement:  splitterpb.NewPlacementServiceClient(cc),
	}
}

func (c *client) ListTenants(ctx context.Context) ([]TenantInfo, error) {
	req := &splitterpb.ListTenantsRequest{}
	resp, err := c.management.ListTenants(ctx, req)
	if err != nil {
		return nil, FromGRPCError(err)
	}
	return slicex.Map(resp.GetTenants(), WrapTenantInfo), nil
}

func (c *client) NewTenant(ctx context.Context, name TenantName, cfg TenantConfig) (TenantInfo, error) {
	req := &splitterpb.NewTenantRequest{
		Name:   string(name),
		Config: UnwrapTenantConfig(cfg),
	}
	resp, err := c.management.NewTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, FromGRPCError(err)
	}
	return WrapTenantInfo(resp.GetTenant()), nil
}

func (c *client) InfoTenant(ctx context.Context, name TenantName) (TenantInfo, error) {
	req := &splitterpb.InfoTenantRequest{Name: string(name)}
	resp, err := c.management.InfoTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, FromGRPCError(err)
	}
	return WrapTenantInfo(resp.GetTenant()), nil
}

func (c *client) UpdateTenant(ctx context.Context, name TenantName, guard Version, opts ...UpdateTenantOption) (TenantInfo, error) {
	req := &splitterpb.UpdateTenantRequest{
		Name:    string(name),
		Version: int64(guard),
	}
	for _, opt := range opts {
		opt(req)
	}

	upd, err := c.management.UpdateTenant(ctx, req)
	if err != nil {
		return TenantInfo{}, FromGRPCError(err)
	}
	return WrapTenantInfo(upd.GetTenant()), nil
}

func (c *client) DeleteTenant(ctx context.Context, name TenantName) error {
	req := &splitterpb.DeleteTenantRequest{Name: string(name)}
	_, err := c.management.DeleteTenant(ctx, req)
	return FromGRPCError(err)
}

func (c *client) ListServices(ctx context.Context, tenant TenantName) ([]ServiceInfoEx, error) {
	req := &splitterpb.ListServicesRequest{
		Tenant: string(tenant),
	}
	resp, err := c.management.ListServices(ctx, req)
	if err != nil {
		return nil, FromGRPCError(err)
	}
	return slicex.Map(resp.GetServices(), WrapServiceInfoEx), nil
}

func (c *client) NewService(ctx context.Context, name QualifiedServiceName, cfg ServiceConfig) (ServiceInfo, error) {
	req := &splitterpb.NewServiceRequest{
		Name:   name.ToProto(),
		Config: UnwrapServiceConfig(cfg),
	}
	resp, err := c.management.NewService(ctx, req)
	if err != nil {
		return ServiceInfo{}, FromGRPCError(err)
	}
	return WrapServiceInfo(resp.GetService()), nil
}

func (c *client) InfoService(ctx context.Context, name QualifiedServiceName) (ServiceInfoEx, error) {
	req := &splitterpb.InfoServiceRequest{Name: name.ToProto()}
	resp, err := c.management.InfoService(ctx, req)
	if err != nil {
		return ServiceInfoEx{}, FromGRPCError(err)
	}
	return WrapServiceInfoEx(resp.GetService()), nil
}

func (c *client) UpdateService(ctx context.Context, name QualifiedServiceName, guard Version, opts ...UpdateServiceOption) (ServiceInfo, error) {
	req := &splitterpb.UpdateServiceRequest{
		Name:    name.ToProto(),
		Version: int64(guard),
	}
	for _, opt := range opts {
		opt(req)
	}

	upd, err := c.management.UpdateService(ctx, req)
	if err != nil {
		return ServiceInfo{}, FromGRPCError(err)
	}
	return WrapServiceInfo(upd.GetService()), nil
}

func (c *client) DeleteService(ctx context.Context, name QualifiedServiceName) error {
	req := &splitterpb.DeleteServiceRequest{Name: name.ToProto()}
	_, err := c.management.DeleteService(ctx, req)
	return FromGRPCError(err)
}

func (c *client) ListDomains(ctx context.Context, service QualifiedServiceName) ([]Domain, error) {
	req := &splitterpb.ListDomainsRequest{
		Service: service.ToProto(),
	}
	resp, err := c.management.ListDomains(ctx, req)
	if err != nil {
		return nil, FromGRPCError(err)
	}
	return slicex.Map(resp.GetDomains(), WrapDomain), nil
}

func (c *client) NewDomain(ctx context.Context, name QualifiedDomainName, domainType DomainType, cfg DomainConfig, opts ...NewDomainOption) (Domain, error) {
	req := &splitterpb.NewDomainRequest{
		Name:   name.ToProto(),
		Type:   domainType,
		Config: UnwrapDomainConfig(cfg),
	}
	for _, opt := range opts {
		opt(req)
	}
	resp, err := c.management.NewDomain(ctx, req)
	if err != nil {
		return Domain{}, FromGRPCError(err)
	}
	return WrapDomain(resp.GetDomain()), nil
}

func (c *client) UpdateDomain(ctx context.Context, name QualifiedDomainName, guard Version, opts ...UpdateDomainOption) (Domain, error) {
	req := &splitterpb.UpdateDomainRequest{
		Name:           name.ToProto(),
		ServiceVersion: int64(guard),
	}
	for _, opt := range opts {
		opt(req)
	}

	upd, err := c.management.UpdateDomain(ctx, req)
	if err != nil {
		return Domain{}, FromGRPCError(err)
	}
	return WrapDomain(upd.GetDomain()), nil
}

func (c *client) DeleteDomain(ctx context.Context, name QualifiedDomainName) error {
	req := &splitterpb.DeleteDomainRequest{Name: name.ToProto()}
	_, err := c.management.DeleteDomain(ctx, req)
	return FromGRPCError(err)
}

func (c *client) ListPlacements(ctx context.Context, name TenantName) ([]PlacementInfo, error) {
	req := &splitterpb.ListPlacementsRequest{
		Tenant: string(name),
	}
	resp, err := c.placement.List(ctx, req)
	if err != nil {
		return nil, FromGRPCError(err)
	}
	return slicex.Map(resp.GetInfo(), WrapPlacementInfo), nil
}

func (c *client) InfoPlacement(ctx context.Context, name QualifiedPlacementName) (PlacementInfo, error) {
	req := &splitterpb.InfoPlacementRequest{
		Name: name.ToProto(),
	}
	resp, err := c.placement.Info(ctx, req)
	if err != nil {
		return PlacementInfo{}, FromGRPCError(err)
	}
	return WrapPlacementInfo(resp.GetInfo()), nil
}
