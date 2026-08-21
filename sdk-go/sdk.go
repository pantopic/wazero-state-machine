package statemachine

const (
	flagPersistent = 1 << iota
	flagStreamable
	flagWatchable
)

type (
	funcOpen         func() uint64
	funcUpdate       func(index uint64, cmd []byte) (value uint64, data []byte)
	funcFinish       func()
	funcRead         func(query []byte) (value uint64, data []byte)
	funcStreamOpen   func()
	funcStreamRecv   func(cmd []byte)
	funcStreamClosed func()
	funcWatchOpen    func(cmd []byte)
	funcWatchClosed  func()
)

var (
	fnOpen         funcOpen
	fnUpdate       funcUpdate
	fnFinish       funcFinish
	fnRead         funcRead
	fnStreamOpen   funcStreamOpen
	fnStreamRecv   funcStreamRecv
	fnStreamClosed funcStreamClosed
	fnWatchOpen    funcWatchOpen
	fnWatchClosed  funcWatchClosed
)

func Concurrent(
	update funcUpdate,
	finish funcFinish,
	read funcRead,
) {
	fnUpdate = update
	fnFinish = finish
	fnRead = read
}

func Persistent(
	open funcOpen,
	update funcUpdate,
	finish funcFinish,
	read funcRead,
) {
	fnOpen = open
	fnUpdate = update
	fnFinish = finish
	fnRead = read
	flags |= flagPersistent
}

func Streaming(
	streamOpen funcStreamOpen,
	streamRecv funcStreamRecv,
	streamClosed funcStreamClosed,
) {
	fnStreamOpen = streamOpen
	fnStreamRecv = streamRecv
	fnStreamClosed = streamClosed
	flags |= flagStreamable
}

func StreamSend(val uint64, data []byte) {
	setValue(val)
	setData(data)
	streamSend()
}

func StreamClose() {
	streamClose()
}

func Watchable(
	watchOpen funcWatchOpen,
	watchClosed funcWatchClosed,
) {
	fnWatchOpen = watchOpen
	fnWatchClosed = watchClosed
	flags = flags & flagWatchable
}

func WatchSend(val uint64, data []byte) {
	setValue(val)
	setData(data)
	watchSend()
}

func WatchClose() {
	watchClose()
}
