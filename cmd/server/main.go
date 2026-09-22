// Command server exposes the ledger over a small JSON API and serves a
// single-page dashboard for driving it: create accounts, move money, and run
// the naive-vs-engine stress test live.
//
// State is durable: the server recovers from a write-ahead log on start and
// appends every change, so restarting keeps the books.
package main

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"

	"github.com/GubinGeramifard/ledger/internal/bench"
	"github.com/GubinGeramifard/ledger/internal/engine"
)

//go:embed web
var webFS embed.FS

var eng *engine.Engine

func main() {
	walPath := env("LEDGER_WAL", "ledger.wal")
	e, err := engine.Recover(walPath, true)
	if err != nil {
		log.Fatalf("recover ledger: %v", err)
	}
	eng = e
	seedIfEmpty(e)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/stats", handleStats)
	mux.HandleFunc("GET /api/accounts", handleAccounts)
	mux.HandleFunc("POST /api/accounts", handleCreate)
	mux.HandleFunc("POST /api/deposit", handleDeposit)
	mux.HandleFunc("POST /api/transfers", handleTransfer)
	mux.HandleFunc("POST /api/stress", handleStress)

	sub, _ := fs.Sub(webFS, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	addr := ":" + env("PORT", "8080")
	log.Printf("ledger listening on %s (wal: %s)", addr, walPath)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, eng.Stats())
}

func handleAccounts(w http.ResponseWriter, r *http.Request) {
	accts := eng.Accounts()
	if accts == nil {
		accts = []engine.Account{}
	}
	writeJSON(w, http.StatusOK, accts)
}

func handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeErr(w, http.StatusBadRequest, "id is required")
		return
	}
	if err := eng.CreateAccount(req.ID); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": req.ID})
}

func handleDeposit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     string `json:"id"`
		Amount string `json:"amount"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	amount, err := engine.ParseMoney(req.Amount)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res := eng.Deposit(req.ID, amount)
	if res.Err != "" {
		writeErr(w, http.StatusUnprocessableEntity, res.Err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func handleTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From    string `json:"from"`
		To      string `json:"to"`
		Amount  string `json:"amount"`
		IdemKey string `json:"idempotency_key"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	amount, err := engine.ParseMoney(req.Amount)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res := eng.Transfer(req.IdemKey, req.From, req.To, amount)
	if res.Err != "" {
		writeErr(w, http.StatusUnprocessableEntity, res.Err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleStress runs the naive-vs-engine comparison on throwaway ledgers (it does
// not touch the live accounts) and returns the full report.
func handleStress(w http.ResponseWriter, r *http.Request) {
	p := bench.Default()
	// Body is optional; ignore decode errors and fall back to defaults.
	_ = json.NewDecoder(r.Body).Decode(&p)
	writeJSON(w, http.StatusOK, bench.Run(p))
}

// seedIfEmpty gives a fresh ledger a few funded accounts so the dashboard opens
// with something to look at.
func seedIfEmpty(e *engine.Engine) {
	if e.Stats().Accounts > 0 {
		return
	}
	seed := []struct {
		id     string
		amount engine.Money
	}{
		{"alice", 500000},
		{"bob", 250000},
		{"carol", 100000},
	}
	for _, s := range seed {
		_ = e.CreateAccount(s.id)
		e.Deposit(s.id, s.amount)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}
