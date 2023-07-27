package raftstorage

import (
	"github.com/hashicorp/raft"
	"io"
)

var (
	_ raft.FSM = (*FSM)(nil)
)

type FSM struct {
}

func NewFSM(path string) (*FSM, error) {
	return &FSM{}, nil
}

func (F FSM) Apply(log *raft.Log) interface{} {
	//TODO implement me
	panic("implement me")
}

func (F FSM) Snapshot() (raft.FSMSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (F FSM) Restore(snapshot io.ReadCloser) error {
	//TODO implement me
	panic("implement me")
}
