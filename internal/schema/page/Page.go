// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package page

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Page struct {
	_tab flatbuffers.Table
}

func GetRootAsPage(buf []byte, offset flatbuffers.UOffsetT) *Page {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Page{}
	x.Init(buf, n+offset)
	return x
}

func FinishPageBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsPage(buf []byte, offset flatbuffers.UOffsetT) *Page {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Page{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedPageBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Page) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Page) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Page) Revision() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Page) MutateRevision(n int64) bool {
	return rcv._tab.MutateInt64Slot(4, n)
}

func (rcv *Page) DataType() Data {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return Data(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Page) MutateDataType(n Data) bool {
	return rcv._tab.MutateByteSlot(6, byte(n))
}

func (rcv *Page) Data(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

func PageStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func PageAddRevision(builder *flatbuffers.Builder, revision int64) {
	builder.PrependInt64Slot(0, revision, 0)
}
func PageAddDataType(builder *flatbuffers.Builder, dataType Data) {
	builder.PrependByteSlot(1, byte(dataType), 0)
}
func PageAddData(builder *flatbuffers.Builder, data flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(data), 0)
}
func PageEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
