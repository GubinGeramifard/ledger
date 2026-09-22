package engine

// Recover rebuilds an engine from the write-ahead log at path, then attaches
// that log so new commands continue to be persisted. If the file does not
// exist, it returns a fresh engine writing to a new log there.
//
// Replay applies each record directly to state without re-logging it, so
// recovery is idempotent: recovering twice yields the same balances.
func Recover(path string, fsync bool) (*Engine, error) {
	records, err := ReadWAL(path)
	if err != nil {
		return nil, err
	}

	e := New()
	e.submit(func() {
		for _, r := range records {
			e.replay(r)
		}
	})

	w, err := OpenWAL(path, fsync)
	if err != nil {
		return nil, err
	}
	e.submit(func() { e.wal = w })
	return e, nil
}

// replay applies a single logged record to state. It trusts the log (the record
// was valid when it was first written) and does not re-validate or re-log.
func (e *Engine) replay(r Record) {
	switch r.Type {
	case recCreate:
		if _, ok := e.accounts[r.Account]; !ok {
			e.accounts[r.Account] = &Account{ID: r.Account}
		}
	case recTransfer:
		src := e.accounts[r.From]
		dst := e.accounts[r.To]
		if src == nil || dst == nil {
			return
		}
		src.Balance -= r.Amount
		dst.Balance += r.Amount
		if r.Seq > e.seq {
			e.seq = r.Seq
		}
		if r.From != ExternalAccount {
			e.count++
		}
		if r.IdemKey != "" {
			e.seen[r.IdemKey] = TransferResult{IdemKey: r.IdemKey, Applied: true}
		}
	}
}
