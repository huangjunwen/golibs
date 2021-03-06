package incrdump

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"

	. "github.com/huangjunwen/golibs/mycanal"
)

// IncrDump reads events from mysql binlog, see mycanal's doc for prerequisites
func IncrDump(
	ctx context.Context,
	cfg *Config,
	gtidSet string,
	handler Handler,
) error {

	conf := cfg.ToBinlogSyncerCfg()
	gset, err := mysql.ParseMysqlGTIDSet(gtidSet)
	if err != nil {
		panic(err)
	}

	syncer := replication.NewBinlogSyncer(conf)
	defer syncer.Close()

	streamer, err := syncer.StartSyncGTID(gset)
	if err != nil {
		return errors.WithMessage(err, "incrdump.IncrDump start sync gtid error")
	}

	var (
		prevGset = gset.Clone()

		// Current trx context, nil if not entered yet.
		trxCtx *TrxContext

		// Remain size of current trx.
		trxRemainSize uint64
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		binlogEvent, err := streamer.GetEvent(ctx)
		if err != nil {
			if err == ctx.Err() {
				err = nil
			}
			return errors.WithMessage(err, "incrdump.IncrDump get event error")
		}

		// Every trx starts with a gtid event.
		if event, ok := binlogEvent.Event.(*replication.GTIDEvent); ok {
			if trxCtx != nil {
				panic(fmt.Errorf(
					"Previous trx(%s) not finish and new trx(%s) starts",
					trxCtx.gtid,
					gtidFromGTIDEvent(event),
				))
			}

			// TransactionLength should be > 0 if version >= 8.0.2
			// https://mysqlhighavailability.com/taking-advantage-of-new-transaction-length-metadata/
			if event.TransactionLength == 0 {
				panic(fmt.Errorf(
					"GTIDEvent has no TransactionLength, pls make sure you are using >= MySQL-8.0.2",
				))
			}

			trxCtx = &TrxContext{
				prevGset:  prevGset.Clone(),
				gtidEvent: event,
				gtid:      gtidFromGTIDEvent(event),
			}
			trxRemainSize = safeUint64Minus(event.TransactionLength, uint64(binlogEvent.Header.EventSize))

			if err := handler(ctx, (*TrxBeginning)(trxCtx)); err != nil {
				return err
			}
			continue
		}

		// NOTE: Ignore other event if not inside trx.
		if trxCtx == nil {
			continue
		}

		switch event := binlogEvent.Event.(type) {

		case *replication.RowsEvent:

			table := event.Table
			if len(table.ColumnName) != int(table.ColumnCount) {
				panic(fmt.Errorf(
					"TableMapEvent has no ColumnName, pls make sure you are using >= MySQL-8.0.1 and set --binlog-row-metadata=FULL",
				))
			}

			// NOTE: We have checked ColumnName above, thus --binlog-row-metadata=FULL should have been enabled.
			meta := newTableMeta(table)

			switch binlogEvent.Header.EventType {
			case replication.WRITE_ROWS_EVENTv2:
				for i := 0; i < len(event.Rows); i++ {
					if err := handler(ctx, &RowInsertion{
						&rowChange{
							trxCtx:     trxCtx,
							rowsEvent:  event,
							meta:       meta,
							beforeData: nil,
							afterData:  meta.NormalizeRowData(event.Rows[i]),
						},
					}); err != nil {
						return err
					}
				}

			case replication.UPDATE_ROWS_EVENTv2:
				for i := 0; i < len(event.Rows); i += 2 {
					if err := handler(ctx, &RowUpdating{
						&rowChange{
							trxCtx:     trxCtx,
							rowsEvent:  event,
							meta:       meta,
							beforeData: meta.NormalizeRowData(event.Rows[i]),
							afterData:  meta.NormalizeRowData(event.Rows[i+1]),
						},
					}); err != nil {
						return err
					}
				}

			case replication.DELETE_ROWS_EVENTv2:
				for i := 0; i < len(event.Rows); i++ {
					if err := handler(ctx, &RowDeletion{
						&rowChange{
							trxCtx:     trxCtx,
							rowsEvent:  event,
							meta:       meta,
							beforeData: meta.NormalizeRowData(event.Rows[i]),
							afterData:  nil,
						},
					}); err != nil {
						return err
					}
				}

			default:
				panic(fmt.Errorf(
					"Expect v2 ROWS_EVENT but got %s event", binlogEvent.Header.EventType.String(),
				))
			}

		default:
		}

		// check trx end.
		trxRemainSize = safeUint64Minus(trxRemainSize, uint64(binlogEvent.Header.EventSize))
		if trxRemainSize > 0 {
			continue
		}

		if err := handler(ctx, (*TrxEnding)(trxCtx)); err != nil {
			return err
		}

		prevGset = trxCtx.AfterGTIDSet().Clone()
		trxCtx = nil

	}

}
