package wazero_state_machine

import (
	"context"
	"io"
	"log/slog"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const Uri = "pantopic/wazero-state-machine"

func Factory(ctx context.Context, logger Logger, modPool PoolProvider) func(shardID, replicaID uint64) zongzi.StateMachine {
	return func(shardID, replicaID uint64) zongzi.StateMachine {
		return &StateMachine{
			ctx:       ctx,
			log:       logger,
			pool:      modPool(shardID),
			shardID:   shardID,
			replicaID: replicaID,
		}
	}
}

var _ zongzi.StateMachine = (*StateMachine)(nil)

type StateMachine struct {
	zongzi.StateMachine

	ctx       context.Context
	log       Logger
	pool      wazeropool.Instance
	replicaID uint64
	shardID   uint64
}

func (fsm *StateMachine) Update(entries []Entry) []Entry {
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		update := mod.ExportedFunction("__state_machine_update")
		defer mod.ExportedFunction("__state_machine_finish").Call(fsm.ctx)
		for i, e := range entries {
			setIndex(mod, meta, e.Index)
			setData(mod, meta, e.Cmd)
			if _, err := update.Call(fsm.ctx); err != nil {
				panic(err)
			}
			entries[i].Result.Value = readUint64(mod, meta.ptrValue)
			entries[i].Result.Data = append(e.Result.Data[:0], getData(mod, meta)...)
			slog.Info(`Update`, `Value`, entries[i].Result.Value, `Data`, string(entries[i].Result.Data))
		}
	})
	return entries
}

func (fsm *StateMachine) Query(ctx context.Context, data []byte) (result *Result) {
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		update := mod.ExportedFunction("__state_machine_read")
		setData(mod, meta, data)
		if _, err := update.Call(fsm.ctx); err != nil {
			panic(err)
		}
		result.Value = readUint64(mod, meta.ptrValue)
		result.Data = append(result.Data[:0], getData(mod, meta)...)
	})
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
