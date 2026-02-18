package statemachine

const (
	flagPersistent = 1 << iota
	flagStreaming
)

type (
	funcOpen         func() uint64
	funcUpdate       func(index uint64, cmd []byte) (value uint64, data []byte)
	funcFinish       func()
	funcRead         func(query []byte) (value uint64, data []byte)
	funcStreamOpen   func()
	funcStreamRecv   func(cmd []byte)
	funcStreamClosed func()
)

var (
	fnOpen         funcOpen
	fnUpdate       funcUpdate
	fnFinish       funcFinish
	fnRead         funcRead
	fnStreamOpen   funcStreamOpen
	fnStreamRecv   funcStreamRecv
	fnStreamClosed funcStreamClosed
)

func Register(
	update funcUpdate,
	finish funcFinish,
	read funcRead,
) {
	fnUpdate = update
	fnFinish = finish
	fnRead = read
}

func RegisterPersistent(
	open funcOpen,
	update funcUpdate,
	finish funcFinish,
	read funcRead,
) {
	fnOpen = open
	fnUpdate = update
	fnFinish = finish
	fnRead = read
	flags = flags & flagPersistent
}

func WithStreaming(
	streamOpen funcStreamOpen,
	streamRecv funcStreamRecv,
	streamClosed funcStreamClosed,
) {
	fnStreamOpen = streamOpen
	fnStreamRecv = streamRecv
	fnStreamClosed = streamClosed
	flags = flags & flagStreaming
}

func StreamSend(val uint64, data []byte) {
	setValue(val)
	setData(data)
	streamSend()
}

func StreamClose() {
	streamClose()
}
