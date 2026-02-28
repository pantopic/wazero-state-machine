package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const UriPersistent = "pantopic/wazero-state-machine/persistent"

type PoolProvider func(shardID uint64) wazeropool.Instance

func FactoryPersistent(ctx context.Context, logger Logger, modPool PoolProvider, ctxCopy ...ctxCopyFunc) func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
	return func(shardID, replicaID uint64) zongzi.StateMachinePersistent {
		return &StateMachinePersistent{
			ctx:       ctx,
			ctxCopy:   ctxCopy,
			log:       logger,
			pool:      modPool(shardID),
			replicaID: replicaID,
			shardID:   shardID,
		}
	}
}

var _ zongzi.StateMachinePersistent = (*StateMachinePersistent)(nil)

type StateMachinePersistent struct {
	zongzi.StateMachinePersistent

	ctx       context.Context
	ctxCopy   []ctxCopyFunc
	log       Logger
	pool      wazeropool.Instance
	replicaID uint64
	shardID   uint64
}

func (fsm *StateMachinePersistent) Open(stopc <-chan struct{}) (index uint64, err error) {
	var stack []uint64
	fsm.pool.Run(func(mod api.Module) {
		if stack, err = mod.ExportedFunction("__state_machine_open").Call(fsm.ctx); err != nil {
			return
		}
		if len(stack) > 0 {
			index = stack[0]
		}
	})
	return
}

func (fsm *StateMachinePersistent) contextCopy(ctx context.Context) context.Context {
	for _, fn := range fsm.ctxCopy {
		ctx = fn(ctx, fsm.ctx)
	}
	return ctx
}

func (fsm *StateMachinePersistent) Update(entries []Entry) []Entry {
	ctx := fsm.contextCopy(context.Background())
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		update := mod.ExportedFunction("__state_machine_update")
		for i, e := range entries {
			setIndex(mod, meta, e.Index)
			setData(mod, meta, e.Cmd)
			if _, err := update.Call(ctx); err != nil {
				panic(err)
			}
			entries[i].Result.Value = readUint64(mod, meta.ptrValue)
			entries[i].Result.Data = append(entries[i].Result.Data[:0], getData(mod, meta)...)
		}
		mod.ExportedFunction("__state_machine_finish").Call(ctx)
	})
	return entries
}

func (fsm *StateMachinePersistent) Query(ctx context.Context, data []byte) (res *Result) {
	ctx = fsm.contextCopy(ctx)
	res = zongzi.GetResult()
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		read := mod.ExportedFunction("__state_machine_read")
		setData(mod, meta, data)
		if _, err := read.Call(ctx); err != nil {
			panic(err)
		}
		res.Value = readUint64(mod, meta.ptrValue)
		res.Data = append(res.Data[:0], getData(mod, meta)...)
	})
	return
}

func (fsm *StateMachinePersistent) Watch(ctx context.Context, data []byte, out chan<- *Result) {
	ctx = fsm.contextCopy(ctx)
	var closed bool
	stop := make(chan bool)
	meta := get[*meta](fsm.ctx, ctxKeyMeta)
	ctx = context.WithValue(ctx, ctxKeySend, func(res *Result) {
		out <- res
	})
	ctx = context.WithValue(ctx, ctxKeyClose, func() {
		closed = true
		close(stop)
	})
	fsm.pool.Run(func(mod api.Module) {
		setData(mod, meta, data)
		mod.ExportedFunction("__state_machine_watch_open").Call(ctx)
	})
	select {
	case <-ctx.Done():
		if !closed {
			close(stop)
		}
		break
	case <-stop:
		break
	}
	fsm.pool.Run(func(mod api.Module) {
		setData(mod, meta, data)
		mod.ExportedFunction("__state_machine_watch_closed").Call(ctx)
	})
}

func (fsm *StateMachinePersistent) Stream(ctx context.Context, in <-chan []byte, out chan<- *Result) {
	ctx = fsm.contextCopy(ctx)
	var closed bool
	stop := make(chan bool)
	meta := get[*meta](fsm.ctx, ctxKeyMeta)
	fsm.pool.Run(func(mod api.Module) {
		mod.ExportedFunction("__state_machine_stream_open").Call(ctx)
	})
	ctx = context.WithValue(ctx, ctxKeySend, func(res *Result) {
		out <- res
	})
	ctx = context.WithValue(ctx, ctxKeyClose, func() {
		closed = true
		close(stop)
	})
loop:
	for {
		select {
		case <-ctx.Done():
			if !closed {
				close(stop)
			}
			break loop
		case <-stop:
			break loop
		case data := <-in:
			fsm.pool.Run(func(mod api.Module) {
				setData(mod, meta, data)
				mod.ExportedFunction("__state_machine_stream_recv").Call(ctx)
			})
		}
	}
	fsm.pool.Run(func(mod api.Module) {
		mod.ExportedFunction("__state_machine_stream_closed").Call(ctx)
	})
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
