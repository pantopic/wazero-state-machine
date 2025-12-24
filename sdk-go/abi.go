package statemachine

import (
	"unsafe"
)

var (
	meta      = make([]uint32, 8)
	flags     uint16
	ShardID   uint64
	ReplicaID uint64
	index     uint64
	value     uint64
	bufCap    uint32 = 2 * 1024 * 1024
	bufLen    uint32
	buf       []byte
)

//export __statemachine
func __statemachine() uint32 {
	buf = make([]byte, int(bufCap))
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

//export __statemachineOpen
func open() uint64 {
	return fnOpen()
}

//export __statemachineUpdate
func update() {
	var tmp []byte
	value, tmp = fnUpdate(index, buf[:int(bufLen)])
	bufLen = uint32(len(tmp))
}

//export __statemachineFinish
func finish() {
	fnFinish()
}

//export __statemachineRead
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
