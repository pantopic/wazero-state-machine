package main

import (
	"bytes"
	"strconv"

	"github.com/pantopic/wazero-state-machine/sdk-go"
)

var (
	idx   uint64
	items uint64
	sets  uint64
)

func main() {
	statemachine.Register(update, finish, read)
	statemachine.Streamable(streamOpen, streamRecv, streamClosed)
	statemachine.Watchable(watchOpen, watchClosed)
}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	items++
	value = idx
	idx = index
	data = bytes.ReplaceAll(cmd, []byte(`test`), []byte(`best`))
	return
}

func finish() {
	sets++
}

var (
	queryIndex = []byte(`index`)
	queryItems = []byte(`items`)
	querySets  = []byte(`sets`)
)

func read(query []byte) (value uint64, data []byte) {
	if bytes.Equal(query, queryIndex) {
		value = idx
	} else if bytes.Equal(query, queryItems) {
		value = items
	} else if bytes.Equal(query, querySets) {
		value = sets
	} else {
		panic(`Unrecognized query: "` + string(query) + `"`)
	}
	return
}

func streamOpen() {
	println(`wasm open`)
}

func streamRecv(data []byte) {
	println(`wasm recv ` + string(data))
	if bytes.Equal(data, []byte(`close`)) {
		println(`wasm close start`)
		statemachine.StreamClose()
		println(`wasm close complete`)
	} else {
		println(`wasm send start`)
		statemachine.StreamSend(1, data)
		println(`wasm send complete`)
	}
}

func streamClosed() {
	println(`wasm closed`)
}

func watchOpen(data []byte) {
	println(`wasm watch open`)
	n, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}
	for i := range n {
		println(`wasm watch send ` + strconv.Itoa(i+1))
		statemachine.WatchSend(1, []byte(strconv.Itoa(i+1)))
	}
	statemachine.WatchClose()
}

func watchClosed() {
	println(`wasm watch closed`)
}
