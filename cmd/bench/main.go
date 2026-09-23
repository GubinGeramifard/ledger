// Command bench runs one concurrent transfer workload against three ledgers,
// naive (no sync), locked (one global mutex), and the single-writer engine, and
// prints what each did to the money and how fast it went.
package main

import (
	"flag"
	"fmt"

	"github.com/GubinGeramifard/ledger/internal/bench"
	"github.com/GubinGeramifard/ledger/internal/engine"
)

func main() {
	p := bench.Default()
	flag.IntVar(&p.Accounts, "accounts", p.Accounts, "number of accounts")
	flag.IntVar(&p.Transfers, "transfers", p.Transfers, "number of unique transfers")
	flag.IntVar(&p.Workers, "workers", p.Workers, "concurrent workers")
	flag.Float64Var(&p.DupFrac, "dupes", p.DupFrac, "fraction of transfers resent as retries")
	trials := flag.Int("trials", 1, "repeat the workload this many times")
	durable := flag.Bool("durable", false, "instead, measure durable throughput (fsync-per-op mutex vs group-commit engine)")
	durN := flag.Int("durable-transfers", 10000, "transfers for the durable benchmark")
	flag.Parse()

	if *durable {
		c, err := bench.RunDurable(*durN, p.Workers)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("Durable throughput: %d transfers, %d workers, real fsync\n\n", c.Transfers, c.Workers)
		fmt.Printf("  mutex (fsync under lock):   %s transfers/sec  (%d ms)\n", commas(c.MutexFsync.TPS), c.MutexFsync.Millis)
		fmt.Printf("  engine (group commit):      %s transfers/sec  (%d ms)\n", commas(c.GroupCommit.TPS), c.GroupCommit.Millis)
		fmt.Printf("\n  Group commit is %.0fx faster once every transfer must survive a crash.\n", c.Speedup)
		return
	}

	for t := 1; t <= *trials; t++ {
		if *trials > 1 {
			fmt.Printf("── trial %d/%d ──\n", t, *trials)
		}
		c := bench.Run(p)
		fmt.Printf("Workload: %d accounts, %d transfers (+%d retries), %d workers\n\n",
			c.Params.Accounts, c.Params.Transfers, c.Retries, c.Params.Workers)

		fmt.Printf("%-8s %13s %9s %9s %16s %9s\n", "", "throughput", "p50", "p99", "money conjured", "verdict")
		row(c.Naive)
		row(c.Locked)
		row(c.Engine)
		fmt.Println()
		fmt.Printf("Naive lost %s to lost updates. Locked and engine both kept the books exact;\n",
			dollars(abs(c.Naive.Drift)))
		fmt.Printf("the difference between them is how they serialize: one global mutex vs a\n")
		fmt.Printf("single-writer state machine (%s/s vs %s/s, p99 %.1fµs vs %.1fµs).\n\n",
			commas(c.Locked.TPS), commas(c.Engine.TPS), c.Locked.P99Micros, c.Engine.P99Micros)
	}
}

func row(r bench.Result) {
	verdict := "LOSES MONEY"
	if r.Correct {
		verdict = "exact"
	}
	fmt.Printf("%-8s %10s/s %7.1fµs %7.1fµs %16s %9s\n",
		r.Name, commas(r.TPS), r.P50Micros, r.P99Micros, "$"+dollars(r.Drift), verdict)
}

func dollars(m engine.Money) string { return m.String() }

func abs(m engine.Money) engine.Money {
	if m < 0 {
		return -m
	}
	return m
}

func commas(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
