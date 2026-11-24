package wildcat

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newShutdownTestDB opens a database tuned so memtables flush quickly and many
// SSTables accumulate, which gives the compactor work to do during shutdown.
func newShutdownTestDB(t *testing.T, dir string) *DB {
	t.Helper()

	opts := &Options{
		Directory:                dir,
		SyncOption:               SyncFull,
		WriteBufferSize:          4 * 1024,
		FlusherTickerInterval:    time.Millisecond,
		CompactorTickerInterval:  25 * time.Millisecond,
		CompactionCooldownPeriod: time.Millisecond,
		CloseTimeout:             10 * time.Second,
		CompactionSizeThreshold:  2,
		MaxCompactionConcurrency: 1,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db
}

// seedSSTables writes enough data to produce several level-1 SSTables and waits
// for the flusher to persist them.
func seedSSTables(t *testing.T, db *DB, batches, keysPerBatch int) {
	t.Helper()

	for batch := 0; batch < batches; batch++ {
		for i := 0; i < keysPerBatch; i++ {
			key := []byte(fmt.Sprintf("b%03d_key%04d", batch, i))
			value := []byte(fmt.Sprintf("b%03d_value%04d", batch, i))
			if err := db.Update(func(txn *Txn) error {
				return txn.Put(key, value)
			}); err != nil {
				t.Fatalf("failed to write key %s: %v", key, err)
			}
		}

		// A value larger than the write buffer forces a memtable rotation/flush.
		largeValue := make([]byte, db.opts.WriteBufferSize)
		if err := db.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("b%03d_large", batch)), largeValue)
		}); err != nil {
			t.Fatalf("failed to write large value: %v", err)
		}
	}

	if err := db.ForceFlush(); err != nil {
		t.Fatalf("force flush failed: %v", err)
	}

	level1 := (*db.levels.Load())[0]
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ssts := level1.sstables.Load(); ssts != nil && len(*ssts) >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for seed SSTables to be flushed")
}

// startBlockedCompaction launches a level 1 -> level 2 compaction of the first
// two level-1 SSTables in a goroutine and blocks it at a test gate until the
// caller closes the returned release channel. This makes shutdown tests
// deterministic: the compaction is guaranteed to be in flight when Close runs,
// regardless of how fast small test datasets merge.
func startBlockedCompaction(t *testing.T, db *DB) (jobDone chan error, release chan struct{}) {
	t.Helper()

	level1 := (*db.levels.Load())[0]
	ssts := *level1.sstables.Load()
	if len(ssts) < 2 {
		t.Fatalf("need at least 2 SSTables for compaction, got %d", len(ssts))
	}

	tables := ssts[:2]
	for _, table := range tables {
		atomic.StoreInt32(&table.isMerging, 1)
	}

	release = make(chan struct{})
	entered := make(chan struct{})
	jobDone = make(chan error, 1)

	db.testGate.Store(&compactionTestGate{
		gate:    release,
		entered: entered,
	})

	go func() {
		jobDone <- db.compactor.compactSSTables(tables, 0, 1)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction never reached the test gate")
	}

	return jobDone, release
}

// TestClose_DuringCompactionBoundsExitTime is the regression test for the
// original issue: closing while a large compaction is in progress used to block
// until the compaction finished. Shutdown must now cancel the compaction
// cooperatively and return within the configured grace period.
func TestClose_DuringCompactionBoundsExitTime(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_compaction")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := newShutdownTestDB(t, dir)
	seedSSTables(t, db, 6, 20)

	// Start a compaction and hold it mid-flight at the test gate.
	jobDone, release := startBlockedCompaction(t, db)

	const gracePeriod = 2 * time.Second
	start := time.Now()
	close(release)
	err = db.CloseWithTimeout(gracePeriod)
	elapsed := time.Since(start)

	if err != nil && !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("unexpected close error: %v", err)
	}

	// The in-flight compaction must stop cooperatively rather than run to completion.
	select {
	case jobErr := <-jobDone:
		if !errors.Is(jobErr, errCompactionCanceled) {
			t.Fatalf("expected errCompactionCanceled, got %v", jobErr)
		}
	case <-time.After(gracePeriod + 2*time.Second):
		t.Fatal("in-flight compaction did not stop after Close")
	}

	if elapsed > gracePeriod+time.Second {
		t.Fatalf("Close took %s, expected at most ~%s", elapsed, gracePeriod)
	}

	t.Logf("Close returned in %s (grace period %s), err=%v", elapsed, gracePeriod, err)
}

// TestClose_TimeoutReturnsSentinelError forces the grace period to be shorter
// than any background work can take and checks ErrCloseTimeout is surfaced.
func TestClose_TimeoutReturnsSentinelError(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_timeout")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := newShutdownTestDB(t, dir)
	seedSSTables(t, db, 4, 20)

	// Hold a compaction mid-flight so the short deadline is guaranteed to expire
	// while background work is still running.
	jobDone, release := startBlockedCompaction(t, db)

	// Use a realistic but tight deadline. The gate stays closed until after Close
	// returns, so workers cannot exit early and beat the timer.
	const timeout = 100 * time.Millisecond
	err = db.CloseWithTimeout(timeout)
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("expected ErrCloseTimeout, got %v", err)
	}

	// Let the gate go; the interrupted compaction must then unwind cooperatively
	// rather than mutating levels while the database is closing.
	close(release)
	select {
	case jobErr := <-jobDone:
		if !errors.Is(jobErr, errCompactionCanceled) {
			t.Fatalf("expected errCompactionCanceled, got %v", jobErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted compaction did not stop after shutdown")
	}
}

// TestClose_PreservesCommittedData verifies the "never throw away in-flight
// writes" requirement: after closing (even via timeout), reopening the database
// replays WAL/SST state and every committed key is still readable.
func TestClose_PreservesCommittedData(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_durability")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	const batches = 5
	const keysPerBatch = 15

	db := newShutdownTestDB(t, dir)
	seedSSTables(t, db, batches, keysPerBatch)
	jobDone, release := startBlockedCompaction(t, db)

	// A short deadline may expire while background work is still winding down.
	close(release)
	closeErr := db.CloseWithTimeout(500 * time.Millisecond)
	if closeErr != nil && !errors.Is(closeErr, ErrCloseTimeout) {
		t.Fatalf("unexpected close error: %v", closeErr)
	}

	// Ensure the interrupted compaction fully exits before reopening the files.
	select {
	case <-jobDone:
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted compaction did not stop after Close")
	}

	// Reopen and verify all committed data survives.
	reopened := newShutdownTestDB(t, dir)
	defer reopened.Close()

	for batch := 0; batch < batches; batch++ {
		for i := 0; i < keysPerBatch; i++ {
			key := []byte(fmt.Sprintf("b%03d_key%04d", batch, i))
			expected := []byte(fmt.Sprintf("b%03d_value%04d", batch, i))

			var got []byte
			err := reopened.View(func(txn *Txn) error {
				v, err := txn.Get(key)
				if err != nil {
					return err
				}
				got = v
				return nil
			})
			if err != nil {
				t.Fatalf("key %s missing after reopen: %v", key, err)
			}
			if string(got) != string(expected) {
				t.Fatalf("key %s: expected %s, got %s", key, expected, got)
			}
		}
	}
}

// TestClose_CleansTempFilesOnReopen ensures an interrupted compaction/flush
// cannot leave .tmp files that confuse later opens.
func TestClose_CleansTempFilesOnReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_tmpfiles")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := newShutdownTestDB(t, dir)
	seedSSTables(t, db, 4, 15)
	jobDone, release := startBlockedCompaction(t, db)

	close(release)
	if err := db.CloseWithTimeout(250 * time.Millisecond); err != nil && !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("unexpected close error: %v", err)
	}

	select {
	case <-jobDone:
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted compaction did not stop after Close")
	}

	// Reopening triggers level reopen, which removes leftover .tmp files.
	reopened := newShutdownTestDB(t, dir)
	defer reopened.Close()

	var leftover []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), TempFileExtension) {
			leftover = append(leftover, path)
		}
		return nil
	})

	if len(leftover) > 0 {
		t.Fatalf("temporary files survived reopen: %v", leftover)
	}
}

// TestClose_IsIdempotent makes repeated Close calls safe for callers such as
// defer-heavy shutdown paths.
func TestClose_IsIdempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_idempotent")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := newShutdownTestDB(t, dir)
	seedSSTables(t, db, 2, 10)

	if err := db.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if err := db.CloseWithTimeout(time.Second); err != nil {
		t.Fatalf("CloseWithTimeout after Close failed: %v", err)
	}
}

// TestClose_DefaultTimeoutApplied checks the options default and the fallback
// when a non-positive timeout is supplied.
func TestClose_DefaultTimeoutApplied(t *testing.T) {
	dir, err := os.MkdirTemp("", "wildcat_shutdown_default_timeout")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(&Options{Directory: dir})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if db.opts.CloseTimeout != DefaultCloseTimeout {
		t.Fatalf("expected default CloseTimeout %s, got %s", DefaultCloseTimeout, db.opts.CloseTimeout)
	}

	if err := db.CloseWithTimeout(0); err != nil {
		t.Fatalf("CloseWithTimeout(0) should fall back to default: %v", err)
	}
}
