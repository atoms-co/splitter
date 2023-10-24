package leader

import (
	"go.atoms.co/splitter/pkg/service/worker"
	"go.atoms.co/splitter/pb/private"
	"github.com/golang/protobuf/proto"
)

type JoinMessage struct {
	pb *internal_v1.JoinMessage
}

type Message struct {
	pb *internal_v1.LeaderMessage
}

func NewWorkerMessage(m worker.Message) Message {
	return Message{pb: &internal_v1.LeaderMessage{
		Msg: &internal_v1.LeaderMessage_Worker{
			Worker: worker.UnwrapMessage(m),
		},
	}}
}

func (m Message) IsWorkerMessage() bool {
	return m.pb.GetWorker() != nil
}

func (m Message) WorkerMessage() (worker.Message, bool) {
	return worker.WrapMessage(m.pb.GetWorker()), m.pb.GetWorker() != nil
}
func (m Message) String() string {
	return proto.MarshalTextString(m.pb)
}
