package ddsutils

import (
	"encoding/binary"
	"sync"
	"unsafe"
)

func StringToByteSlice(s *string) []byte {
	return *(*[]byte)(unsafe.Pointer(s))
}

func ByteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func Uint64ToInt64(val uint64) int64 {
	return *(*int64)(unsafe.Pointer(&val))
}

func Uint64ToFloat64(val uint64) float64 {
	return *(*float64)(unsafe.Pointer(&val))
}

func Int64ToUint64(val int64) uint64 {
	return *(*uint64)(unsafe.Pointer(&val))
}

func Float64ToUint64(val float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&val))
}

var (
	convertByte = sync.Pool{New: func() interface{} { return make([]byte, 4) }}
)

func UInt16ToBytes(i uint16) []byte {
	td := convertByte.Get().([]byte)
	binary.LittleEndian.PutUint16(td, i)
	convertByte.Put(td)
	return convertByte.Get().([]byte)
}
