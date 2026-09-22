package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDurabilityAfterCrash simulates a crash: the engine writes transfers with
// fsync durability but is never cleanly closed (the process just dies). A fresh
// engine recovering from the same log must reproduce every balance exactly. This
// is the guarantee that matters for money, an acknowledged transfer survives a
// crash.
func TestDurabilityAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.wal")

	e1, err := Recover(path, true) // fsync on
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, e1, "alice", "bob")
	e1.Deposit("alice", 100_00)
	e1.Transfer("t1", "alice", "bob", 30_00)
	e1.Transfer("t2", "alice", "bob", 10_00)
	// Simulate the process being killed: no clean flush, just release the handle.
	// With fsync on, every acknowledged write is already on disk.
	e1.simulateCrash()

	e2, err := Recover(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if bal, _ := e2.Balance("alice"); bal != 60_00 {
		t.Errorf("after crash, alice = %s, want 60.00", bal)
	}
	if bal, _ := e2.Balance("bob"); bal != 40_00 {
		t.Errorf("after crash, bob = %s, want 40.00", bal)
	}
	if s := e2.Stats(); s.Total != 0 {
		t.Errorf("after crash, total = %s, want 0", s.Total)
	}
}

// TestRecoveryFromTornLog simulates the classic crash artifact: the process died
// midway through writing the last record, leaving a partial final line. Recovery
// must apply every complete record and simply ignore the torn tail, rather than
// failing to start or corrupting state.
func TestRecoveryFromTornLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.wal")

	e1, err := Recover(path, true)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, e1, "alice", "bob")
	e1.Deposit("alice", 100_00)
	e1.Transfer("t1", "alice", "bob", 25_00)
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a half-written record, as a crash mid-append would leave behind.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":99,"type":"transfer","from":"alice","to":"b`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e2, err := Recover(path, true)
	if err != nil {
		t.Fatalf("recovery failed on a torn log: %v", err)
	}
	defer e2.Close()
	if bal, _ := e2.Balance("bob"); bal != 25_00 {
		t.Errorf("bob = %s, want 25.00 (torn record must be ignored, complete ones applied)", bal)
	}
	if s := e2.Stats(); s.Total != 0 {
		t.Errorf("total = %s, want 0", s.Total)
	}
}
