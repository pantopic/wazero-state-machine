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
	buf       = make([]byte, int(bufCap))
	tmp       []byte
)

//export __state_machine
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

//export __state_machine_open
func open() uint64 {
	return fnOpen()
}

//export __state_machine_update
func update() {
	value, tmp = fnUpdate(index, buf[:int(bufLen)])
	copy(buf[:len(tmp)], tmp)
	bufLen = uint32(len(tmp))
}

//export __state_machine_finish
func finish() {
	fnFinish()
}

//export __state_machine_read
func read() {
	value, tmp = fnRead(buf[:int(bufLen)])
	copy(buf[:len(tmp)], tmp)
	bufLen = uint32(len(tmp))
}

var _ = __statemachine
var _ = open
var _ = update
var _ = finish
var _ = read
