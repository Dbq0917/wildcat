package wildcat

import (
	"os"
	"testing"

	"github.com/wildcatdb/wildcat/v2/blockmanager"
)

func TestReinstateIgnoresRolledBackWritesWhenSlotIsReused(t *testing.T) {
	dir, err := os.MkdirTemp("", "reinstate_slot_reuse")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(&Options{
		Directory:              dir,
		SyncOption:             SyncFull,
		MaxConcurrentTxns:      1,
		RecoverUncommittedTxns: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	rolledBack, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.Put([]byte("rolled_back"), []byte("leak")); err != nil {
		t.Fatal(err)
	}
	rolledBackID := rolledBack.Id
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}

	committed, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if committed.Id != rolledBackID {
		t.Fatalf("expected transaction slot %d to be reused, got %d", rolledBackID, committed.Id)
	}
	if err := committed.Put([]byte("committed"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(&Options{
		Directory:              dir,
		SyncOption:             SyncFull,
		RecoverUncommittedTxns: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if err := db2.Update(func(txn *Txn) error {
		if _, err := txn.Get([]byte("rolled_back")); err == nil {
			t.Fatal("rolled back key is visible after restart")
		}
		if _, err := txn.Get([]byte("committed")); err != nil {
			t.Fatalf("committed key missing after restart: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReinstateRecoverUncommittedDoesNotExposeRows(t *testing.T) {
	dir, err := os.MkdirTemp("", "reinstate_recover_uncommitted")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(&Options{Directory: dir, SyncOption: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Put([]byte("uncommitted"), []byte("leak")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(&Options{
		Directory:              dir,
		SyncOption:             SyncFull,
		RecoverUncommittedTxns: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	recovered, err := db2.GetTxn(0)
	if err != nil {
		t.Fatalf("uncommitted transaction should remain available for inspection: %v", err)
	}
	if recovered.Committed {
		t.Fatal("recovered transaction should not be marked committed")
	}

	if err := db2.Update(func(txn *Txn) error {
		if _, err := txn.Get([]byte("uncommitted")); err == nil {
			t.Fatal("uncommitted key is visible after restart")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReinstateRecognizesCommitMarkerWithoutWriteSet(t *testing.T) {
	dir, err := os.MkdirTemp("", "reinstate_commit_marker")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	walPath := dir + string(os.PathSeparator) + "1.wal"
	wal, err := blockmanager.Open(walPath, os.O_RDWR|os.O_CREATE, DefaultPermission, blockmanager.SyncFull)
	if err != nil {
		t.Fatal(err)
	}

	writeEntry := &Txn{
		Id:        7,
		Timestamp: 100,
		WriteSet:  map[string][]byte{"committed_by_marker": []byte("value")},
		DeleteSet: make(map[string]bool),
		ReadSet:   make(map[string]int64),
	}
	writeData, err := writeEntry.serializeTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(writeData); err != nil {
		t.Fatal(err)
	}

	commitEntry := &Txn{
		Id:        7,
		Timestamp: 100,
		WriteSet:  make(map[string][]byte),
		DeleteSet: make(map[string]bool),
		ReadSet:   make(map[string]int64),
		Committed: true,
	}
	commitData, err := commitEntry.serializeTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(commitData); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(&Options{Directory: dir, SyncOption: SyncFull})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.View(func(txn *Txn) error {
		value, err := txn.Get([]byte("committed_by_marker"))
		if err != nil {
			return err
		}
		if string(value) != "value" {
			t.Fatalf("expected value, got %q", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
