package sqlh

import (
	"context"
	"database/sql"
	"errors"
)

var (
	// Rollback is used to rollback a transaction without returning an error.
	Rollback = errors.New("Just rollback")
)

var (
	emptyTxOptions = &TxOptions{}
)

// TxOptions contains extra options for a db transaction.
type TxOptions struct {
	sql.TxOptions

	// BeforeTx will be called (if not nil) before transaction starts.
	// `conn` is the db session used to start transaction.
	// If the callback returns an error, WithTxOpts returns that error and the transaction
	// will not start.
	BeforeTx func(ctx context.Context, conn *sql.Conn) error

	// AfterTx will be called (if not nil) after transaction finished with the commit status.
	// `conn` is the db session used to start transaction.
	AfterTx func(ctx context.Context, conn *sql.Conn, committed bool)
}

// WithTx starts a transaction and run fn. If no error is returned by fn, the transaction will be committed.
// Otherwise it is rollbacked and the error is returned to the caller (except returning Rollback,
// which will rollback the transaction but not returning error).
func WithTx(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) (err error) {
	return WithTxOpts(ctx, db, nil, fn)
}

// WithTxOpts is similar to WithTx with extra options.
func WithTxOpts(ctx context.Context, db *sql.DB, opts *TxOptions, fn func(context.Context, *sql.Tx) error) (err error) {

	if opts == nil {
		opts = emptyTxOptions
	}

	beginTx := db.BeginTx
	txCommitted := false

	if opts.BeforeTx != nil || opts.AfterTx != nil {
		// If any hook is not nil, then use a *sql.Conn explicitly.
		var conn *sql.Conn
		conn, err = db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		if opts.BeforeTx != nil {
			if err = opts.BeforeTx(ctx, conn); err != nil {
				return err
			}
		}
		if opts.AfterTx != nil {
			defer func() {
				opts.AfterTx(ctx, conn, txCommitted)
			}()
		}

		beginTx = conn.BeginTx
	}

	tx, err := beginTx(ctx, &opts.TxOptions)
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
		if err == Rollback {
			// Rollback is not treated as an error.
			err = nil
		}
	}()

	err = fn(ctx, tx)
	if err != nil {
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}
	txCommitted = true
	return
}
