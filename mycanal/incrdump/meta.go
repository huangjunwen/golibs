package incrdump

import (
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
	unsignedMap     map[int]bool
	enumStrValueMap map[int][]string
	setStrValueMap  map[int][]string
}

func newTableMeta(table *replication.TableMapEvent) *tableMeta {
	return &tableMeta{
		TableMapEvent: table,
	}
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
