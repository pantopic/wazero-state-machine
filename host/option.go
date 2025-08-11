package wazero_state_machine

type Option func(*hostModule)

func WithCtxKeyMeta(key string) Option {
	return func(p *hostModule) {
		p.ctxKeyMeta = key
	}
}
