package sqlh

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"testing"

	tstmysql "github.com/huangjunwen/tstsvc/mysql"
	"github.com/stretchr/testify/assert"
)

var (
	testTxErr       = errors.New("test tx error")
	testBeforeTxErr = errors.New("test before tx error")
)

func getDBSessionId(ctx context.Context, q Queryer) (id int64, err error) {
	err = q.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	return
}

func mustGetDBSessionId(ctx context.Context, q Queryer) int64 {
	id, err := getDBSessionId(ctx, q)
	if err != nil {
		panic(err)
	}
	return id
}

func getLock(ctx context.Context, q Queryer, lock string) (r sql.NullInt64, err error) {
	err = q.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lock).Scan(&r)
	return
}

func releaseLock(ctx context.Context, q Queryer, lock string) (r sql.NullInt64, err error) {
	err = q.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", lock).Scan(&r)
	return
}

func isUsedLock(ctx context.Context, q Queryer, lock string) (id sql.NullInt64, err error) {
	err = q.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", lock).Scan(&id)
	return
}

func TestWithTxOpts(t *testing.T) {
	log.Printf("\n")
	log.Printf(">>> TestWithTxOpts.\n")
	var err error
	assert := assert.New(t)

	// Starts test mysql server.
	var resMySQL *tstmysql.Resource
	{
		resMySQL, err = tstmysql.Run(nil)
		if err != nil {
			log.Panic(err)
		}
		defer resMySQL.Close()
		log.Printf("MySQL server started.\n")
	}

	// Connects to test mysql server.
	var db *sql.DB
	{
		db, err = resMySQL.Client()
		if err != nil {
			log.Panic(err)
		}
		defer db.Close()
		log.Printf("MySQL client created.\n")
	}

	{
		_, err := db.Exec("CREATE DATABASE IF NOT EXISTS xxxx")
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Test database created.\n")

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS xxxx.t (name VARCHAR(64) PRIMARY KEY)")
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Test table created.\n")
	}

	bgctx := context.Background()

	for _, testCase := range []struct {
		Fn         func(context.Context, *sql.Tx) error
		Opts       *TxOptions
		ExpectErr  error
		ExtraCheck func()
	}{
		// Commit without options.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO xxxx.t (name) values ('aaaa')")
				assert.NoError(err)
				return err
			},
			ExpectErr: nil,
			ExtraCheck: func() {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM xxxx.t WHERE name='aaaa'").Scan(&count)
				assert.NoError(err)
				assert.Equal(1, count)
			},
		},
		// Rollback without options.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO xxxx.t (name) values ('aaaa')")
				assert.NoError(err)
				return Rollback
			},
			ExpectErr: nil,
			ExtraCheck: func() {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM xxxx.t WHERE name='aaaa'").Scan(&count)
				assert.NoError(err)
				assert.Equal(0, count)
			},
		},
		// Error rollback without options.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO xxxx.t (name) values ('aaaa')")
				assert.NoError(err)
				return testTxErr
			},
			ExpectErr: testTxErr,
			ExtraCheck: func() {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM xxxx.t WHERE name='aaaa'").Scan(&count)
				assert.NoError(err)
				assert.Equal(0, count)
			},
		},
		// BeforeTx returns error.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				return nil
			},
			Opts: &TxOptions{
				BeforeTx: func(ctx context.Context, conn *sql.Conn) error {
					return testBeforeTxErr
				},
			},
			ExpectErr: testBeforeTxErr,
		},
		// Commit with options.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				// The lock should be held (by BeforeTx).
				id, err := isUsedLock(ctx, tx, "test.lock")
				assert.NoError(err)
				assert.True(id.Valid)

				// And it can aquire the lock since the tx resides in the same session.
				r, err := getLock(ctx, tx, "test.lock")
				assert.NoError(err)
				assert.True(r.Valid)
				assert.Equal(int64(1), r.Int64)
				return nil
			},
			Opts: &TxOptions{
				BeforeTx: func(ctx context.Context, conn *sql.Conn) error {
					r, err := getLock(ctx, conn, "test.lock")
					assert.NoError(err)
					assert.True(r.Valid)
					assert.Equal(int64(1), r.Int64)
					return nil
				},
				AfterTx: func(ctx context.Context, conn *sql.Conn, committed bool) {
					assert.True(committed)

					r, err := releaseLock(ctx, conn, "test.lock")
					assert.NoError(err)
					assert.True(r.Valid)
					assert.Equal(int64(1), r.Int64)
				},
			},
			ExpectErr: nil,
		},
		// Rollback with options.
		{
			Fn: func(ctx context.Context, tx *sql.Tx) error {
				return testTxErr
			},
			Opts: &TxOptions{
				BeforeTx: func(ctx context.Context, conn *sql.Conn) error {
					return nil
				},
				AfterTx: func(ctx context.Context, conn *sql.Conn, committed bool) {
					assert.False(committed)
				},
			},
			ExpectErr: testTxErr,
		},
	} {
		// Always clean test table first.
		_, err := db.Exec("DELETE FROM xxxx.t")
		if err != nil {
			log.Panic(err)
		}

		err = WithTxOpts(bgctx, db, testCase.Opts, testCase.Fn)
		assert.Equal(testCase.ExpectErr, err)
		if testCase.ExtraCheck != nil {
			testCase.ExtraCheck()
		}
	}

}
