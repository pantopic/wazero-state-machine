package guest

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
)

const StateMachinePersistentUri = "zongzi://pantopic.com/cluster/guest/persistent"

func NewStateMachinePersistent(logger Logger, shardID, replicaID uint64) *StateMachinePersistent {
	return &StateMachinePersistent{
		log:       logger,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

var _ zongzi.StateMachinePersistent = (*StateMachinePersistent)(nil)

type StateMachinePersistent struct {
	zongzi.StateMachinePersistent

	log       Logger
	shardID   uint64
	replicaID uint64
}

func (fsm *StateMachinePersistent) Open(stopc <-chan struct{}) (index uint64, err error) {
	return
}

func (fsm *StateMachinePersistent) Update(entries []Entry) []Entry {
	return entries
}

func (fsm *StateMachinePersistent) Query(ctx context.Context, data []byte) (result *Result) {
	return
}

func (fsm *StateMachinePersistent) PrepareSnapshot() (cursor any, err error) {
	return
}

func (fsm *StateMachinePersistent) SaveSnapshot(cursor any, w io.Writer, close <-chan struct{}) (err error) {
	return
}

func (fsm *StateMachinePersistent) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) (err error) {
	return
}

func (sm *StateMachinePersistent) Sync() (err error) {
	return
}

func (fsm *StateMachinePersistent) Close() (err error) {
	return
}
