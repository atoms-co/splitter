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
	"go.atoms.co/lib/iox"
	"go.atoms.co/splitter/pkg/model"
	"go.atoms.co/splitter/pkg/service/leader"
	"go.atoms.co/splitter/pb/private"
	"fmt"
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
	quit := iox.NewAsyncCloser()
	defer quit.Close()

	wctx, _ := contextx.WithQuitCancel(server.Context(), quit.Closed()) // cancel context if session server closes

	return grpcx.Receive(wctx, server, func(ctx context.Context, in <-chan *internal_v1.JoinMessage) (<-chan *internal_v1.JoinMessage, error) {
		// Read session initialization message
		establish, err := session.ReadEstablish(in, func(m *internal_v1.JoinMessage) (session.Message, bool) {
			if m.GetSession() != nil {
				return session.WrapMessage(m.GetSession()), true
			}
			return session.Message{}, false
		})
		if err != nil {
			log.Errorf(ctx, "Unabled to establish a session: %v", err)
			return nil, model.WrapError(fmt.Errorf("%v: %w", err, model.ErrInvalid))
		}

		log.Infof(ctx, "Received establish for sid %v, (client: %v -> self: %v)", establish.ID, establish.Client, l.self)

		sess, out, established := session.NewServer(ctx, l.cl, l.self, establish)
		iox.WhenClosed(sess, quit)

		ch := chanx.MapIf(in, func(pb *internal_v1.JoinMessage) (leader.Message, bool) {
			if pb.GetSession() != nil {
				sess.Observe(ctx, session.WrapMessage(pb.GetSession())) // inject into session server
				return leader.Message{}, false
			}
			return leader.WrapMessage(pb.GetLeader()), true
		})

		// Send established message to the coordinator
		err = server.Send(leader.UnwrapJoinMessage(leader.NewJoinSessionMessage(established)))
		if err != nil {
			log.Warnf(ctx, "Send failed: %v", err)
			return nil, model.WrapError(fmt.Errorf("send failed: %w", model.ErrInvalid))
		}

		// Let leader handle join

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
