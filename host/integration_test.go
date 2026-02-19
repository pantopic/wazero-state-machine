package wazero_state_machine

import (
	"bytes"
	"context"
	_ "embed"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/pantopic/wazero-pool"
)

//go:embed test\.wasm
var testWasm []byte

//go:embed test\-persistent\.wasm
var testWasmPersistent []byte

type StateMachineCommon interface {
	Update(entries []Entry) []Entry
	Query(ctx context.Context, query []byte) *Result
	Watch(ctx context.Context, query []byte, result chan<- *Result)
	Stream(ctx context.Context, in <-chan []byte, out chan<- *Result)
}

func TestHostModule(t *testing.T) {
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig())
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	var hostModule *hostModule
	t.Run(`register`, func(t *testing.T) {
		hostModule = New()
		hostModule.Register(ctx, r)
	})

	cfg := wazero.NewModuleConfig().WithStdout(os.Stdout)
	t.Run(`in-memory`, func(t *testing.T) {
		pool, err := wazeropool.New(ctx, r, testWasm,
			wazeropool.WithModuleConfig(cfg))
		if err != nil {
			t.Fatalf(`%v`, err)
		}
		pool.Run(func(mod api.Module) {
			ctx, err = hostModule.InitContext(ctx, mod)
			if err != nil {
				t.Fatalf(`%v`, err)
			}
		})
		smf := Factory(ctx, zongzi.GetLogger(`test`), func(uint64) wazeropool.Instance {
			return pool
		})
		test(t, ctx, smf(1, 1))
	})
	t.Run(`persistent`, func(t *testing.T) {
		pool, err := wazeropool.New(ctx, r, testWasmPersistent,
			wazeropool.WithModuleConfig(cfg))
		if err != nil {
			t.Fatalf(`%v`, err)
		}
		pool.Run(func(mod api.Module) {
			ctx, err = hostModule.InitContext(ctx, mod)
			if err != nil {
				t.Fatalf(`%v`, err)
			}
		})
		smf := FactoryPersistent(ctx, zongzi.GetLogger(`test-persistent`), func(uint64) wazeropool.Instance {
			return pool
		})
		test(t, ctx, smf(1, 1))
	})
}

func test(t *testing.T, ctx context.Context, sm StateMachineCommon) {
	t.Run(`update`, func(t *testing.T) {
		ents := sm.Update([]Entry{{
			Index: 1,
			Cmd:   []byte(`test-1`),
		}, {
			Index: 2,
			Cmd:   []byte(`test-2`),
		}})
		if ents[0].Result.Value != 0 {
			t.Fatalf(`Value 1 should be 0 but got %d`, ents[0].Result.Value)
		}
		if !bytes.Equal(ents[0].Result.Data, []byte(`best-1`)) {
			t.Fatalf(`Data should be "best-1" but got "%s"`, string(ents[0].Result.Data))
		}
		if ents[1].Result.Value != 1 {
			t.Fatalf(`Value 2 should be 1 but got %d`, ents[1].Result.Value)
		}
		if !bytes.Equal(ents[1].Result.Data, []byte(`best-2`)) {
			t.Fatalf(`Data should be "best-2" but got "%s"`, string(ents[1].Result.Data))
		}
	})
	t.Run(`read`, func(t *testing.T) {
		res := sm.Query(ctx, []byte(`items`))
		if res.Value != 2 {
			t.Fatalf(`Value should be 2 but got %d`, res.Value)
		}
		res = sm.Query(ctx, []byte(`sets`))
		if res.Value != 1 {
			t.Fatalf(`Value should be 1 but got %d`, res.Value)
		}
		res = sm.Query(ctx, []byte(`index`))
		if res.Value != 2 {
			t.Fatalf(`Value should be 2 but got %d`, res.Value)
		}
	})
	t.Run(`stream`, func(t *testing.T) {
		t.Run(`success`, func(t *testing.T) {
			in := make(chan []byte)
			out := make(chan *Result)
			go func() {
				in <- []byte(`echo`)
				in <- []byte(`close`)
			}()
			go func() {
				for res := range out {
					if res.Value != 1 {
						t.Fatalf(`Value should be 1 but got %d`, res.Value)
					}
					if string(res.Data) != `echo` {
						t.Fatalf(`Data should be "echo" but got "%s"`, res.Data)
					}
				}
			}()
			sm.Stream(ctx, in, out)
		})
		t.Run(`context cancel`, func(t *testing.T) {
			in := make(chan []byte)
			out := make(chan *Result)
			ctx, cancel := context.WithCancel(ctx)
			go func() {
				in <- []byte(`echo`)
				cancel()
			}()
			go func() {
				for res := range out {
					if res.Value != 1 {
						t.Fatalf(`Value should be 1 but got %d`, res.Value)
					}
					if string(res.Data) != `echo` {
						t.Fatalf(`Data should be "echo" but got "%s"`, res.Data)
					}
				}
			}()
			sm.Stream(ctx, in, out)
		})
	})
	t.Run(`watch`, func(t *testing.T) {
		t.Run(`success`, func(t *testing.T) {
			var received int
			out := make(chan *Result)
			var wg sync.WaitGroup
			wg.Go(func() {
				for res := range out {
					received++
					println(`watch ` + string(res.Data))
					if res.Value != 1 {
						t.Fatalf(`Value should be 1 but got %d`, res.Value)
					}
					if string(res.Data) != strconv.Itoa(received) {
						t.Fatalf(`Data should be "%d" but got "%s"`, received, res.Data)
					}
				}
			})
			n := 2
			sm.Watch(ctx, []byte(strconv.Itoa(n)), out)
			close(out)
			wg.Wait()
			if received != n {
				t.Fatalf(`Should have received %d events but got %d`, n, received)
			}
		})
		t.Run(`context cancel`, func(t *testing.T) {
			var received int
			out := make(chan *Result)
			ctx, cancel := context.WithCancel(ctx)
			var wg sync.WaitGroup
			wg.Go(func() {
				defer close(out)
				for res := range out {
					received++
					if res.Value != 1 {
						t.Fatalf(`Value should be 1 but got %d`, res.Value)
					}
					if string(res.Data) != strconv.Itoa(received) {
						t.Fatalf(`Data should be "%d" but got "%s"`, received, res.Data)
					}
					cancel()
					return
				}
			})
			n := 2
			sm.Watch(ctx, []byte(strconv.Itoa(n)), out)
			wg.Wait()
			if received != 1 {
				t.Fatalf(`Should have received 1 events but got %d`, received)
			}
		})
	})
}
