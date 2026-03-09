package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const Uri = "pantopic/wazero-state-machine"

func Factory(ctx context.Context, logger Logger, modPool PoolProvider, ctxCopy ...ctxCopyFunc) func(shardID, replicaID uint64) zongzi.StateMachine {
	return func(shardID, replicaID uint64) zongzi.StateMachine {
		for _, fn := range ctxCopy {
			ctx = fn(ctx, ctx)
		}
		return &StateMachine{
			ctx:       ctx,
			ctxCopy:   ctxCopy,
			log:       logger,
			pool:      modPool(shardID),
			replicaID: replicaID,
			shardID:   shardID,
		}
	}
}

var _ zongzi.StateMachine = (*StateMachine)(nil)

type StateMachine struct {
	zongzi.StateMachine

	ctx       context.Context
	ctxCopy   []ctxCopyFunc
	log       Logger
	pool      wazeropool.Instance
	replicaID uint64
	shardID   uint64
}

func (fsm *StateMachine) contextCopy(ctx context.Context) context.Context {
	for _, fn := range fsm.ctxCopy {
		ctx = fn(ctx, fsm.ctx)
	}
	return ctx
}

func (fsm *StateMachine) Update(entries []Entry) []Entry {
	ctx := fsm.contextCopy(context.Background())
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
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

func (fsm *StateMachine) Query(ctx context.Context, data []byte) (res *Result) {
	res = zongzi.GetResult()
	ctx = fsm.contextCopy(ctx)
	fsm.pool.Run(func(mod api.Module) {
		meta := get[*meta](fsm.ctx, ctxKeyMeta)
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
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

func (fsm *StateMachine) Watch(ctx context.Context, data []byte, out chan<- *Result) {
	var closed bool
	stop := make(chan bool)
	meta := get[*meta](fsm.ctx, ctxKeyMeta)
	ctx = fsm.contextCopy(ctx)
	ctx = context.WithValue(ctx, ctxKeySend, func(res *Result) {
		out <- res
	})
	ctx = context.WithValue(ctx, ctxKeyClose, func() {
		closed = true
		close(stop)
	})
	fsm.pool.Run(func(mod api.Module) {
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
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
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
		setData(mod, meta, data)
		mod.ExportedFunction("__state_machine_watch_closed").Call(ctx)
	})
}

func (fsm *StateMachine) Stream(ctx context.Context, in <-chan []byte, out chan<- *Result) {
	var closed bool
	stop := make(chan bool)
	meta := get[*meta](fsm.ctx, ctxKeyMeta)
	ctx = fsm.contextCopy(ctx)
	ctx = context.WithValue(ctx, ctxKeySend, func(res *Result) {
		out <- res
	})
	ctx = context.WithValue(ctx, ctxKeyClose, func() {
		closed = true
		close(stop)
	})
	fsm.pool.Run(func(mod api.Module) {
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
		mod.ExportedFunction("__state_machine_stream_open").Call(ctx)
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
				setShardID(mod, meta, fsm.shardID)
				setReplicaID(mod, meta, fsm.replicaID)
				setData(mod, meta, data)
				mod.ExportedFunction("__state_machine_stream_recv").Call(ctx)
			})
		}
	}
	fsm.pool.Run(func(mod api.Module) {
		setShardID(mod, meta, fsm.shardID)
		setReplicaID(mod, meta, fsm.replicaID)
		mod.ExportedFunction("__state_machine_stream_closed").Call(ctx)
	})
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

func (fsm *StateMachine) Close() (err error) {
	return
}
