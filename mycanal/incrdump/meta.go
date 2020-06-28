package incrdump

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var (
	emptyUnsignedMap     = map[int]bool{}
	emptyEnumStrValueMap = map[int][]string{}
	emptySetStrValueMap  = map[int][]string{}
)

type tableMeta struct {
	*replication.TableMapEvent
	// cache fields
	schemaName      string
	tableName       string
	unsignedMap     map[int]bool
	enumStrValueMap map[int][]string
	setStrValueMap  map[int][]string
}

func newTableMeta(table *replication.TableMapEvent) *tableMeta {
	return &tableMeta{
		TableMapEvent: table,
	}
}

func (meta *tableMeta) SchemaName() string {
	if meta.schemaName == "" {
		meta.schemaName = string(meta.TableMapEvent.Schema)
	}
	return meta.schemaName
}

func (meta *tableMeta) TableName() string {
	if meta.tableName == "" {
		meta.tableName = string(meta.TableMapEvent.Table)
	}
	return meta.tableName
}

func (meta *tableMeta) UnsignedMap() map[int]bool {
	if meta.unsignedMap == nil {
		meta.unsignedMap = meta.TableMapEvent.UnsignedMap()
		if meta.unsignedMap == nil {
			meta.unsignedMap = emptyUnsignedMap
		}
	}
	return meta.unsignedMap
}

func (meta *tableMeta) EnumStrValueMap() map[int][]string {
	if meta.enumStrValueMap == nil {
		meta.enumStrValueMap = meta.TableMapEvent.EnumStrValueMap()
		if meta.enumStrValueMap == nil {
			meta.enumStrValueMap = emptyEnumStrValueMap
		}
	}
	return meta.enumStrValueMap
}

func (meta *tableMeta) SetStrValueMap() map[int][]string {
	if meta.setStrValueMap == nil {
		meta.setStrValueMap = meta.TableMapEvent.SetStrValueMap()
		if meta.setStrValueMap == nil {
			meta.setStrValueMap = emptySetStrValueMap
		}
	}
	return meta.setStrValueMap
}

// I didn't export TableMapEvent.realType in go-mysql but need to use it here ....
// So copy https://github.com/siddontang/go-mysql/replication/row_event.go
func (meta *tableMeta) RealType(i int) byte {
	typ := meta.TableMapEvent.ColumnType[i]

	switch typ {
	case MYSQL_TYPE_STRING:
		rtyp := byte(meta.TableMapEvent.ColumnMeta[i] >> 8)
		if rtyp == MYSQL_TYPE_ENUM || rtyp == MYSQL_TYPE_SET {
			return rtyp
		}

	case MYSQL_TYPE_DATE:
		return MYSQL_TYPE_NEWDATE
	}

	return typ

}

func (meta *tableMeta) NormalizeRowData(data []interface{}) []interface{} {

	for i, val := range data {

		// No need to handle nil.
		if val == nil {
			continue
		}

		// NOTE: go-mysql stores int as signed values since before MySQL-8, no signedness
		// information is presents in binlog. So we need to convert here if it is unsigned.
		if meta.IsNumericColumn(i) {
			if v, ok := val.(decimal.Decimal); ok {
				data[i] = v.String()
				continue
			}

			if !meta.UnsignedMap()[i] {
				continue
			}

			// Copy from go-mysql/canal/rows.go
			switch v := val.(type) {
			case int8:
				data[i] = uint8(v)

			case int16:
				data[i] = uint16(v)

			case int32:
				if v < 0 && meta.RealType(i) == MYSQL_TYPE_INT24 {
					// 16777215 is the maximum value of mediumint
					data[i] = uint32(16777215 + v + 1)
				} else {
					data[i] = uint32(v)
				}

			case int64:
				data[i] = uint64(v)

			case int:
				data[i] = uint(v)

			default:
				// float/double ...
			}
			continue
		}

		if meta.IsEnumColumn(i) {
			v, ok := val.(int64)
			if !ok {
				panic(fmt.Errorf("Expect int64 for enum (MYSQL_TYPE_ENUM) field but got %T %#v", val, val))
			}
			data[i] = meta.EnumStrValueMap()[i][int(v)-1]
			continue
		}

		if meta.IsSetColumn(i) {
			v, ok := val.(int64)
			if !ok {
				panic(fmt.Errorf("Expect int64 for set (MYSQL_TYPE_SET) field but got %T %#v", val, val))
			}
			setStrValue := meta.SetStrValueMap()[i]
			vals := []string{}
			for j := 0; j < 64; j++ {
				if (v & (1 << uint(j))) != 0 {
					vals = append(vals, setStrValue[j])
				}
			}
			data[i] = strings.Join(vals, ",")
			continue
		}

		if meta.RealType(i) == MYSQL_TYPE_YEAR {
			v, ok := val.(int)
			if !ok {
				panic(fmt.Errorf("Expect int for year (MYSQL_TYPE_YEAR) field but got %T %#v", val, val))
			}
			// NOTE: Convert to uint16 to keep the same as fulldump.
			data[i] = uint16(v)
			continue
		}

		if meta.RealType(i) == MYSQL_TYPE_NEWDATE {
			v, ok := val.(string)
			if !ok {
				panic(fmt.Errorf("Expect string for date (MYSQL_TYPE_NEWDATE) field but got %T %#v", val, val))
			}
			// NOTE: Convert to time.Time to keep the same as fulldump.
			t, err := time.Parse("2006-01-02", v)
			if err != nil {
				panic(err)
			}
			data[i] = t
			continue
		}

		switch v := val.(type) {
		case time.Time:
			data[i] = v.UTC()

		case []byte:
			data[i] = string(v)
		}
	}

	return data
}
