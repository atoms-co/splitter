package leader

import "go.atoms.co/splitter/pb/private"

type HandleRequest struct {
	pb *internal_v1.LeaderHandleRequest
}

type JoinMessage struct {
	pb *internal_v1.JoinMessage
}
