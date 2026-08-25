package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

type Data struct {
	Columns      DataColumns
	SkipByte     int
	ColumnNumber uint16
}

// DataColumns holds columns by value, not by pointer: one contiguous
// allocation instead of one heap object per column.
type DataColumns []DataColumn

type DataColumn struct {
	Data     []byte
	Length   uint32
	DataType uint8
}

type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

func NewData(data []byte, tupleDataType uint8, skipByteLength int) (*Data, error) {
	if data[skipByteLength] != tupleDataType {
		return nil, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	d := &Data{}
	d.Decode(data, skipByteLength)

	return d, nil
}

// Decode parses one row's column headers and values out of data, starting at
// skipByteLength. data is the connection's shared read buffer -- valid only
// until the next message is received -- so every non-null column's payload
// must still be copied out here, never aliased.
//
// d.Columns is preallocated once instead of grown via repeated append, and
// each column is written in place instead of boxed in a separate *DataColumn
// (see DataColumns) -- removing one heap allocation per column, which showed
// up as a dominant contributor in CDC ingestion heap profiles under high WAL
// throughput. The per-column payload copy is unchanged: it is still required
// because data is reused by the connection after this call returns.
func (d *Data) Decode(data []byte, skipByteLength int) {
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	d.Columns = make(DataColumns, d.ColumnNumber)
	for i := range d.Columns {
		col := &d.Columns[i]
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			col.Data = make([]byte, int(col.Length))
			copy(col.Data, data[skipByteLength:])

			skipByteLength += int(col.Length)
		}
	}
	d.SkipByte = skipByteLength
}

func (d *Data) DecodeWithColumn(columns []RelationColumn) (map[string]any, error) {
	decoded := make(map[string]any, d.ColumnNumber)
	for idx, col := range d.Columns {
		colName := columns[idx].Name
		switch col.DataType {
		case DataTypeNull:
			decoded[colName] = nil
		case DataTypeText:
			val, err := decodeTextColumnData(col.Data, columns[idx].DataType)
			if err != nil {
				return nil, errors.Wrap(err, "decode column")
			}
			decoded[colName] = val
		}
	}

	return decoded, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
