package frontend

import (
	"context"
	"atoms.co/lib-go/pkg/clock"
	"go.atoms.co/splitter/lib/service/location"
	"go.atoms.co/splitter/lib/service/session"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/contextx"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"fmt"
	"time"
)

type LeaderService struct {
	cl    clock.Clock
	self  location.Instance
	proxy leader.Proxy
}

func NewLeaderService(cl clock.Clock, loc location.Location, proxy leader.Proxy) *LeaderService {
	return &LeaderService{
		cl:    cl,
		self:  location.NewNamedInstance("leaderProxy", loc),
		proxy: proxy,
	}
}

func (l *LeaderService) Join(server internal_v1.LeaderService_JoinServer) error {
	sess, out := session.NewServer(server.Context(), l.cl, l.self)
	defer sess.Close()
	wctx, _ := contextx.WithQuitCancel(server.Context(), sess.Closed()) // cancel context if session server closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *internal_v1.JoinMessage) (<-chan *internal_v1.JoinMessage, error) {
		ch := chanx.MapIf(in, func(pb *internal_v1.JoinMessage) (leader.Message, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session server
				return leader.Message{}, false
			}
			return leader.WrapMessage(pb.GetLeader()), true
		})

		// (1) Read session initialization message
		establish, ok := chanx.TryRead(sess.Establish(), 20*time.Second)
		if !ok {
			log.Errorf(ctx, "No session establish message received")
			return nil, model.WrapError(fmt.Errorf("no session establish message: %w", model.ErrInvalid))
		}

		// (2) Let leader handle join

		resp, err := l.proxy.Join(ctx, establish.ID, ch)
		if err != nil {
			if !model.IsOwnershipError(err) {
				log.Warnf(ctx, "Internal: join from %v rejected: %v", establish, err)
			}
			return nil, model.WrapError(err)
		}

		joined := session.Receive(sess, chanx.Map(resp, leader.NewJoinMessage), out, leader.NewJoinSessionMessage)
		return chanx.Map(joined, leader.UnwrapJoinMessage), nil
	})
}

func (l *LeaderService) Handle(ctx context.Context, request *internal_v1.LeaderHandleRequest) (*internal_v1.LeaderHandleResponse, error) {
	return l.proxy.Handle(ctx, leader.HandleRequest{Proto: request})
}
