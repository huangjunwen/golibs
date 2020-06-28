package incrdump

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/replication"
)

func safeUint64Minus(left, right uint64) uint64 {
	if left >= right {
		return left - right
	}
	panic(fmt.Errorf("%d < %d", left, right))
}

func gtidFromGTIDEvent(e *replication.GTIDEvent) string {
	return fmt.Sprintf(
		"%s:%d",
		uuid.Must(uuid.FromBytes(e.SID)).String(),
		e.GNO,
	)
}
