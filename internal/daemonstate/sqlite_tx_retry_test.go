package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

var txRetryStubDriverSeq atomic.Uint64

func TestSQLiteWithTxRetryRetriesBusyCallbackError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	store := &SQLiteStore{db: openTxRetryStubDB(t, nil, nil)}

	if err := store.withTxRetry(context.Background(), func(*sql.Tx) error {
		if calls.Add(1) == 1 {
			return errors.New("database is locked")
		}
		return nil
	}); err != nil {
		t.Fatalf("withTxRetry() error = %v", err)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("withTxRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteWithTxRetryRetriesBusyBeginError(t *testing.T) {
	t.Parallel()

	var (
		beginCalls    atomic.Int32
		callbackCalls atomic.Int32
	)
	store := &SQLiteStore{
		db: openTxRetryStubDB(t, func() error {
			if beginCalls.Add(1) == 1 {
				return errors.New("SQLITE_BUSY")
			}
			return nil
		}, nil),
	}

	if err := store.withTxRetry(context.Background(), func(*sql.Tx) error {
		callbackCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("withTxRetry() error = %v", err)
	}
	if got, want := beginCalls.Load(), int32(2); got != want {
		t.Fatalf("begin call count = %d, want %d", got, want)
	}
	if got, want := callbackCalls.Load(), int32(1); got != want {
		t.Fatalf("callback call count = %d, want %d", got, want)
	}
}

func TestSQLiteWithTxRetryRetriesBusyCommitError(t *testing.T) {
	t.Parallel()

	var (
		callbackCalls atomic.Int32
		commitCalls   atomic.Int32
	)
	store := &SQLiteStore{
		db: openTxRetryStubDB(t, nil, func() error {
			if commitCalls.Add(1) == 1 {
				return errors.New("SQLITE_BUSY")
			}
			return nil
		}),
	}

	if err := store.withTxRetry(context.Background(), func(*sql.Tx) error {
		callbackCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("withTxRetry() error = %v", err)
	}
	if got, want := callbackCalls.Load(), int32(2); got != want {
		t.Fatalf("callback call count = %d, want %d", got, want)
	}
	if got, want := commitCalls.Load(), int32(2); got != want {
		t.Fatalf("commit call count = %d, want %d", got, want)
	}
}

func TestSQLiteWithTxRetryReturnsContextErrorWhenWaitIsCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	store := &SQLiteStore{db: openTxRetryStubDB(t, nil, nil)}

	err := store.withTxRetry(ctx, func(*sql.Tx) error {
		calls.Add(1)
		cancel()
		return errors.New("database is locked")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("withTxRetry() error = %v, want %v", err, context.Canceled)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("withTxRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteWithTxRetryReturnsNonBusyErrorImmediately(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	var calls atomic.Int32
	store := &SQLiteStore{db: openTxRetryStubDB(t, nil, nil)}

	err := store.withTxRetry(context.Background(), func(*sql.Tx) error {
		calls.Add(1)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("withTxRetry() error = %v, want %v", err, wantErr)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("withTxRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteWithTxRetryReturnsLastBusyErrorAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("SQLITE_BUSY")
	var calls atomic.Int32
	store := &SQLiteStore{db: openTxRetryStubDB(t, nil, nil)}

	err := store.withTxRetry(context.Background(), func(*sql.Tx) error {
		calls.Add(1)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("withTxRetry() error = %v, want %v", err, wantErr)
	}
	if got, want := calls.Load(), int32(sqliteBusyRetryMaxAttempts); got != want {
		t.Fatalf("withTxRetry() call count = %d, want %d", got, want)
	}
}

func openTxRetryStubDB(t *testing.T, beginFn func() error, commitFn func() error) *sql.DB {
	t.Helper()

	driverName := strings.NewReplacer("/", "_", " ", "_").Replace(
		fmt.Sprintf("tx-retry-%s-%d", t.Name(), txRetryStubDriverSeq.Add(1)),
	)
	sql.Register(driverName, txRetryStubDriver{
		beginFn:  beginFn,
		commitFn: commitFn,
	})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", driverName, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

type txRetryStubDriver struct {
	beginFn  func() error
	commitFn func() error
}

func (d txRetryStubDriver) Open(string) (driver.Conn, error) {
	return txRetryStubConn{
		beginFn:  d.beginFn,
		commitFn: d.commitFn,
	}, nil
}

type txRetryStubConn struct {
	beginFn  func() error
	commitFn func() error
}

func (c txRetryStubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare not implemented")
}

func (c txRetryStubConn) Close() error {
	return nil
}

func (c txRetryStubConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c txRetryStubConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.beginFn != nil {
		if err := c.beginFn(); err != nil {
			return nil, err
		}
	}
	return txRetryStubTx{commitFn: c.commitFn}, nil
}

type txRetryStubTx struct {
	commitFn func() error
}

func (t txRetryStubTx) Commit() error {
	if t.commitFn != nil {
		return t.commitFn()
	}
	return nil
}

func (txRetryStubTx) Rollback() error {
	return nil
}
