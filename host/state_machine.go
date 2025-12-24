package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const Uri = "pantopic/wazero-state-machine"

func Factory(logger Logger) func(shardID, replicaID uint64) zongzi.StateMachine {
	return func(shardID, replicaID uint64) zongzi.StateMachine {
		return &StateMachine{
			log:       logger,
			shardID:   shardID,
			replicaID: replicaID,
		}
	}
}

var _ zongzi.StateMachine = (*StateMachine)(nil)

type StateMachine struct {
	zongzi.StateMachine

	pool      wazeropool.Instance
	ctx       context.Context
	log       Logger
	shardID   uint64
	replicaID uint64
}

func (fsm *StateMachine) Update(entries []Entry) []Entry {
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, DefaultCtxKeyMeta)
		update := mod.ExportedFunction("update")
		for _, e := range entries {
			setData(mod, meta, e.Cmd)
			if _, err := update.Call(fsm.ctx); err != nil {
				panic(err)
			}
			copy(e.Result.Data, getData(mod, meta))
			e.Result.Value = readUint64(mod, meta.ptrValue)
		}
	})
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
