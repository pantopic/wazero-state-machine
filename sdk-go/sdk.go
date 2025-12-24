package statemachine

const (
	flagPersistent = 1
)

type (
	openFunc   func() uint64
	updateFunc func(index uint64, cmd []byte) (value uint64, data []byte)
	finishFunc func()
	readFunc   func(query []byte) (value uint64, data []byte)
)

var (
	fnOpen   openFunc
	fnUpdate updateFunc
	fnFinish finishFunc
	fnRead   readFunc
)

func Register(update updateFunc, finish finishFunc, read readFunc) {
	fnUpdate = update
	fnFinish = finish
	fnRead = read
}

func RegisterPersistent(open openFunc, update updateFunc, finish finishFunc, read readFunc) {
	fnOpen = open
	fnUpdate = update
	fnFinish = finish
	fnRead = read
	flags = flagPersistent
}
