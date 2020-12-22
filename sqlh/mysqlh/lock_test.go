package mysqlh

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/huangjunwen/golibs/logr/zerologr"
	tstmysql "github.com/huangjunwen/tstsvc/mysql"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func killLockConn(db *sql.DB, lockStr string) (id int32, err error) {
	id = -1

	var connId sql.NullInt32
	err = db.QueryRow("SELECT IS_USED_LOCK(?)", lockStr).Scan(&connId)
	if err != nil {
		return
	}

	if !connId.Valid {
		return
	}

	_, err = db.Exec("kill ?", connId.Int32)
	if err != nil {
		return
	}

	id = connId.Int32
	return
}

func TestWithLock(t *testing.T) {
	log.Printf("\n")
	log.Printf(">>> TestWithLock.\n")
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

	// A closed client.
	var dbBad *sql.DB
	{
		dbBad, err = resMySQL.Client()
		if err != nil {
			log.Panic(err)
		}
		dbBad.Close()
	}

	// Change default logger.
	{
		logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
		LockOptDefaultLogger = (*zerologr.Logger)(&logger)
	}

	bgCtx := context.Background()

	// Test bad db.
	log.Printf("\n")
	log.Printf(">>>> Test bad db.\n")
	{
		called := false
		err := WithLock(bgCtx, dbBad, "test.bad.db", func(ctx context.Context) {
			called = true
			<-ctx.Done()
		})
		assert.Error(err)
		assert.False(called)

	}

	// Test context done.
	log.Printf("\n")
	log.Printf(">>>> Test context done.\n")
	{
		called := false
		ctx, _ := context.WithTimeout(bgCtx, 5*time.Second)
		err := WithLock(ctx, db, "test.context.done", func(ctx context.Context) {
			called = true
			<-ctx.Done()
		})
		assert.NoError(err)
		assert.True(called)
	}

	// Test acquired.
	log.Printf("\n")
	log.Printf(">>>> Test acquired.\n")
	{
		called := false
		ctx, cancel := context.WithCancel(bgCtx)
		err := WithLock(ctx, db, "test.acquired", func(ctx context.Context) {

			// Another instance failed to get the lock.
			go func() {
				err := WithLock(bgCtx, db, "test.acquired", func(ctx context.Context) {
					panic("Should not here")
				})
				assert.Equal(LockNotAcquired, err)
				cancel()
			}()

			called = true
			<-ctx.Done()
		})

		assert.NoError(err)
		assert.True(called)
	}

	// Test ping error.
	log.Printf("\n")
	log.Printf(">>>> Test ping error.\n")
	{
		called := false
		err := WithLock(bgCtx, db, "test.ping.error", func(ctx context.Context) {

			// Try to kill the connection.
			go func() {
				time.Sleep(time.Second)

				id, err := killLockConn(db, "test.ping.error")
				assert.NoError(err)
				assert.Greater(id, int32(0))

				log.Printf(">>>>!! killed conn %d\n", id)
			}()

			called = true
			<-ctx.Done()
		})

		assert.NoError(err)
		assert.True(called)
	}

}
