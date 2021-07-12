package incrdump

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
	uuid "github.com/satori/go.uuid"
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
