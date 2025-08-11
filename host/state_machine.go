package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
)

const StateMachineUri = "pantopic/wazero-state-machine"

func NewStateMachine(logger Logger, shardID, replicaID uint64) *StateMachine {
	return &StateMachine{
		log:       logger,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

var _ zongzi.StateMachine = (*StateMachine)(nil)

type StateMachine struct {
	zongzi.StateMachine

	log       Logger
	shardID   uint64
	replicaID uint64
}

func (fsm *StateMachine) Update(entries []Entry) []Entry {
	return entries
}

func (fsm *StateMachine) Query(ctx context.Context, data []byte) (result *Result) {
	return
}

func (fsm *StateMachine) PrepareSnapshot() (cursor any, err error) {
	return
}

func (fsm *StateMachine) SaveSnapshot(cursor any, w io.Writer, _ SnapshotFileCollection, close <-chan struct{}) (err error) {
	return
}

func (fsm *StateMachine) RecoverFromSnapshot(r io.Reader, _ []SnapshotFile, _ <-chan struct{}) (err error) {
	return
}

func (fsm *StateMachine) Lookup(any) (res any, err error) {
	return
}

func (fsm *StateMachine) Close() (err error) {
	return
}
