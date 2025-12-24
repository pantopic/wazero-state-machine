package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const UriPersistent = "pantopic/wazero-state-machine/persistent"

type PoolProvider interface {
	Get(shardID uint64) wazeropool.Instance
}

func FactoryPersistent(logger Logger, provider PoolProvider) func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
	return func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
		pool := provider.Get(shardID)
		return &StateMachinePersistent{
			log:       logger,
			pool:      pool,
			replicaID: replicaID,
			shardID:   shardID,
		}
	}
}

var _ zongzi.StateMachinePersistent = (*StateMachinePersistent)(nil)

type StateMachinePersistent struct {
	zongzi.StateMachinePersistent

	ctx       context.Context
	log       Logger
	pool      wazeropool.Instance
	replicaID uint64
	shardID   uint64
}

func (fsm *StateMachinePersistent) Open(stopc <-chan struct{}) (index uint64, err error) {
	var stack []uint64
	fsm.pool.Run(func(mod api.Module) {
		if stack, err = mod.ExportedFunction("__statemachineInit").Call(fsm.ctx); err != nil {
			return
		}
		if len(stack) > 0 {
			index = stack[0]
		}
	})
	return
}

func (fsm *StateMachinePersistent) Update(entries []Entry) []Entry {
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, DefaultCtxKeyMeta)
		update := mod.ExportedFunction("__statemachineUpdate")
		defer mod.ExportedFunction("__statemachineFinish").Call(fsm.ctx)
		for _, e := range entries {
			setIndex(mod, meta, e.Index)
			setData(mod, meta, e.Cmd)
			if _, err := update.Call(fsm.ctx); err != nil {
				panic(err)
			}
			e.Result.Value = readUint64(mod, meta.ptrValue)
			e.Result.Data = append(e.Result.Data[:0], getData(mod, meta)...)
		}
	})
	return entries
}

func (fsm *StateMachinePersistent) Query(ctx context.Context, data []byte) (result *Result) {
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, DefaultCtxKeyMeta)
		update := mod.ExportedFunction("__statemachineQuery")
		setData(mod, meta, data)
		if _, err := update.Call(fsm.ctx); err != nil {
			panic(err)
		}
		result.Value = readUint64(mod, meta.ptrValue)
		result.Data = append(result.Data[:0], getData(mod, meta)...)
	})
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
