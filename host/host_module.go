package wazero_state_machine

import (
	"context"
	"log"
	"sync"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const Name = "pantopic/wazero-state-machine"

var (
	ctxKeyMeta  = Name + `/meta`
	ctxKeySend  = Name + `/send`
	ctxKeyClose = Name + `/close`
)

type meta struct {
	ptrFlags     uint32
	ptrShardID   uint32
	ptrReplicaID uint32
	ptrIndex     uint32
	ptrValue     uint32
	ptrBufCap    uint32
	ptrBufLen    uint32
	ptrBuf       uint32
}

type hostModule struct {
	sync.RWMutex

	module api.Module
}

func New(opts ...Option) *hostModule {
	p := &hostModule{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *hostModule) Name() string {
	return Name
}

// Register instantiates the host module, making it available to all module instances in this runtime
func (p *hostModule) Register(ctx context.Context, r wazero.Runtime) (err error) {
	builder := r.NewHostModuleBuilder(Name)
	register := func(name string, fn func(ctx context.Context, m api.Module, stack []uint64)) {
		builder = builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(fn), nil, nil).Export(name)
	}
	for name, fn := range map[string]any{
		"__state_machine_stream_send": func(ctx context.Context, res *Result) {
			get[func(*Result)](ctx, ctxKeySend)(res)
		},
		"__state_machine_stream_close": func(ctx context.Context) {
			get[func()](ctx, ctxKeyClose)()
		},
		"__state_machine_watch_send": func(ctx context.Context, res *Result) {
			get[func(*Result)](ctx, ctxKeySend)(res)
		},
		"__state_machine_watch_close": func(ctx context.Context) {
			get[func()](ctx, ctxKeyClose)()
		},
	} {
		switch fn := fn.(type) {
		case func(ctx context.Context, res *Result):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				res := zongzi.GetResult()
				res.Value = getValue(m, meta)
				res.Data = append(res.Data, getData(m, meta)...)
				fn(ctx, res)
			})
		case func(ctx context.Context):
			register(name, func(ctx context.Context, m api.Module, stack []uint64) {
				fn(ctx)
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
	p.module, err = builder.Instantiate(ctx)
	return
}

// InitContext retrieves the meta page from the wasm module
func (p *hostModule) InitContext(ctx context.Context, m api.Module) (context.Context, error) {
	stack, err := m.ExportedFunction(`__state_machine`).Call(ctx)
	if err != nil {
		return ctx, err
	}
	meta := &meta{}
	ptr := uint32(stack[0])
	for i, v := range []*uint32{
		&meta.ptrFlags,
		&meta.ptrShardID,
		&meta.ptrReplicaID,
		&meta.ptrIndex,
		&meta.ptrValue,
		&meta.ptrBufCap,
		&meta.ptrBufLen,
		&meta.ptrBuf,
	} {
		*v = readUint32(m, ptr+uint32(4*i))
	}
	return context.WithValue(ctx, ctxKeyMeta, meta), nil
}

// ContextCopy populates dst context with the meta page from src context.
func (h *hostModule) ContextCopy(dst, src context.Context) context.Context {
	dst = context.WithValue(dst, ctxKeyMeta, get[*meta](src, ctxKeyMeta))
	return dst
}

func (p *hostModule) Stop() (err error) {
	return
}

func get[T any](ctx context.Context, key string) T {
	v := ctx.Value(key)
	if v == nil {
		log.Panicf("Context item missing %s", key)
	}
	return v.(T)
}

func setIndex(m api.Module, meta *meta, index uint64) {
	writeUint64(m, meta.ptrIndex, index)
}

func setData(m api.Module, meta *meta, buf []byte) {
	copy(getBuf(m, meta)[:len(buf)], buf)
	writeUint32(m, meta.ptrBufLen, uint32(len(buf)))
}

func getBuf(m api.Module, meta *meta) []byte {
	return readBytes(m, meta.ptrBuf, 0, meta.ptrBufCap)
}

func getData(m api.Module, meta *meta) []byte {
	return readBytes(m, meta.ptrBuf, meta.ptrBufLen, meta.ptrBufCap)
}

func getValue(m api.Module, meta *meta) (val uint64) {
	return readUint64(m, meta.ptrValue)
}

func readBytes(m api.Module, ptr, ptrLen, ptrCap uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptr, readUint32(m, ptrCap))
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptr, ptrLen)
	}
	return buf[:readUint32(m, ptrLen)]
}

func readUint32(m api.Module, ptr uint32) (val uint32) {
	val, ok := m.Memory().ReadUint32Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func readUint64(m api.Module, ptr uint32) (val uint64) {
	val, ok := m.Memory().ReadUint64Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func writeUint32(m api.Module, ptr uint32, val uint32) {
	if ok := m.Memory().WriteUint32Le(ptr, val); !ok {
		log.Panicf("Memory.Write(%d) out of range", ptr)
	}
}

func writeUint64(m api.Module, ptr uint32, val uint64) {
	if ok := m.Memory().WriteUint64Le(ptr, val); !ok {
		log.Panicf("Memory.Write(%d) out of range", ptr)
	}
}
