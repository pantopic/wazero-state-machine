package statemachine

import (
	"unsafe"
)

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

var (
	flags     uint16
	ShardID   uint64
	ReplicaID uint64
	index     uint64
	value     uint64
	bufCap    uint32 = 2 * 1024 * 1024
	bufLen    uint32
	buf       = make([]byte, int(bufCap))
	meta      = make([]uint32, 8)
)

//export __statemachine
func __statemachine() uint32 {
	meta[0] = uint32(uintptr(unsafe.Pointer(&flags)))
	meta[1] = uint32(uintptr(unsafe.Pointer(&ShardID)))
	meta[2] = uint32(uintptr(unsafe.Pointer(&ReplicaID)))
	meta[3] = uint32(uintptr(unsafe.Pointer(&index)))
	meta[4] = uint32(uintptr(unsafe.Pointer(&value)))
	meta[5] = uint32(uintptr(unsafe.Pointer(&bufCap)))
	meta[6] = uint32(uintptr(unsafe.Pointer(&bufLen)))
	meta[7] = uint32(uintptr(unsafe.Pointer(&buf[0])))
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

//export statemachineOpen
func open() uint64 {
	return fnOpen()
}

//export statemachineUpdate
func update() {
	var tmp []byte
	value, tmp = fnUpdate(index, buf[:int(bufLen)])
	bufLen = uint32(len(tmp))
}

//export statemachineFinish
func finish() {
	fnFinish()
}

//export statemachineRead
func read() {
	var tmp []byte
	value, tmp = fnRead(buf[:int(bufLen)])
	bufLen = uint32(len(tmp))
}

var _ = __statemachine
var _ = open
var _ = update
var _ = finish
var _ = read
