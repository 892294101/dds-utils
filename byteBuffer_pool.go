package ddsutils

import (
	"bytes"
	"sync"
)

const (
	TooBigBlockSize = 1024 * 1024 * 4
)

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// 完整的数据行缓存
var (
	DataRowsBufferPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 数据行缓存（包括操作、行头、元数据、行数据）
)

func DataRowsBufferGet() (data *bytes.Buffer) {
	data = DataRowsBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func DataRowsBufferPut(data *bytes.Buffer) {
	if data == nil || data.Len() > TooBigBlockSize {
		return
	}
	DataRowsBufferPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	MetaDataBufferPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 元数据缓存
)

func MetaDataBufferGet() (data *bytes.Buffer) {
	data = MetaDataBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func MetaDataBufferPut(data *bytes.Buffer) {
	MetaDataBufferPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	HeadDataBufferPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 头数据缓存
)

func HeadBufferGet() (data *bytes.Buffer) {
	data = HeadDataBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func HeadBufferPut(data *bytes.Buffer) {
	MetaDataBufferPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	OuterBufferPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 列嵌套外缓存
)

func OuterBufferGet() (data *bytes.Buffer) {
	data = OuterBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func OuterBufferPut(data *bytes.Buffer) {
	OuterBufferPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	RowBufferPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 行缓存
)

func RowBufferGet() (data *bytes.Buffer) {
	data = RowBufferPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func RowBufferPut(data *bytes.Buffer) {
	RowBufferPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	NestedBytesPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }} // 列嵌套内缓存
)

func NestedBufferGet() (data *bytes.Buffer) {
	data = NestedBytesPool.Get().(*bytes.Buffer)
	//data.Grow(5120)
	data.Reset()
	return data
}

func NestedBufferPut(data *bytes.Buffer) {
	NestedBytesPool.Put(data)
}

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var (
	ConvertPool = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}
)

func ConvertPoolGet() (data *bytes.Buffer) {
	data = ConvertPool.Get().(*bytes.Buffer)
	data.Reset()
	return data
}

func ConvertPoolPut(data *bytes.Buffer) {
	ConvertPool.Put(data)
}
