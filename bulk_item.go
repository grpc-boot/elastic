package belastic

import (
	"strings"

	"github.com/grpc-boot/base"
)

const (
	OptIndex = iota
	OptCreate
	OptUpdate
	OptDelete
)

type BulkItem struct {
	cmd        uint8
	index      string
	id         string
	fieldValue base.JsonParam
}

func IndexItem(index, id string, fieldValue base.JsonParam) BulkItem {
	return BulkItem{cmd: OptIndex, index: index, id: id, fieldValue: fieldValue}
}

func CreateItem(index string, fieldValue base.JsonParam) BulkItem {
	return BulkItem{cmd: OptCreate, index: index, fieldValue: fieldValue}
}

func UpdateItem(index, id string, fieldValue base.JsonParam) BulkItem {
	return BulkItem{cmd: OptUpdate, index: index, id: id, fieldValue: fieldValue}
}

func DeleteItem(index, id string) BulkItem {
	return BulkItem{cmd: OptDelete, index: index, id: id}
}

func (bi *BulkItem) Build() string {
	switch bi.cmd {
	case OptDelete:
		return bi.buildDelete()
	case OptCreate:
		return bi.buildCreate()
	case OptUpdate:
		return bi.buildUpdate()
	}

	return bi.buildIndex()
}

func (bi *BulkItem) buildIndex() string {
	var buf strings.Builder

	fv := bi.fieldValue.JsonMarshal()

	n := 20 + len(bi.index) + 3 + 1 + len(fv)
	if len(bi.id) > 0 {
		n += 9 + len(bi.id)
	}

	buf.Grow(n)

	buf.WriteString(`{"index":{"_index":"`)
	buf.WriteString(bi.index)

	if len(bi.id) > 0 {
		buf.WriteString(`","_id":"`)
		buf.WriteString(bi.id)
	}

	buf.WriteString(`"}}`)

	buf.WriteByte('\n')
	buf.Write(fv)

	return buf.String()
}

func (bi *BulkItem) buildDelete() string {
	var buf strings.Builder
	n := 21 + len(bi.index) + 9 + len(bi.id) + 3

	buf.Grow(n)

	buf.WriteString(`{"delete":{"_index":"`)
	buf.WriteString(bi.index)
	buf.WriteString(`","_id":"`)
	buf.WriteString(bi.id)
	buf.WriteString(`"}}`)

	return buf.String()
}

func (bi *BulkItem) buildCreate() string {
	var buf strings.Builder

	fv := bi.fieldValue.JsonMarshal()

	n := 21 + len(bi.index) + 3 + 1 + len(fv)
	if len(bi.id) > 0 {
		n += 9 + len(bi.id)
	}

	buf.Grow(n)

	buf.WriteString(`{"create":{"_index":"`)
	buf.WriteString(bi.index)

	if len(bi.id) > 0 {
		buf.WriteString(`","_id":"`)
		buf.WriteString(bi.id)
	}

	buf.WriteString(`"}}`)

	buf.WriteByte('\n')
	buf.Write(fv)

	return buf.String()
}

func (bi *BulkItem) buildUpdate() string {
	var buf strings.Builder

	fv := bi.fieldValue.JsonMarshal()

	n := 21 + len(bi.index) + 9 + len(bi.id) + 3 + 1 + len(fv)

	buf.Grow(n)

	buf.WriteString(`{"update":{"_index":"`)
	buf.WriteString(bi.index)
	buf.WriteString(`","_id":"`)
	buf.WriteString(bi.id)
	buf.WriteString(`"}}`)

	buf.WriteByte('\n')
	buf.Write(fv)

	return buf.String()
}
