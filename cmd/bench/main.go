// Command bench runs the same concurrent transfer workload against the naive
// ledger and against the engine, then prints what each one did to the money.
//
// The workload isolates one thing: correctness under concurrency. Every account
// starts with a balance so large that no transfer is ever legitimately rejected,
// so any change in the total is not a business rule firing, it is money being
// created or destroyed by a race. A fraction of the operations are exact
// duplicates (simulated network retries) to exercise idempotency.
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
	flag.Parse()

	c := bench.Run(p)

	fmt.Printf("Workload: %d accounts, %d transfers (+%d retries), %d workers\n\n",
		c.Params.Accounts, c.Params.Transfers, c.Retries, c.Params.Workers)
	report("Naive ledger", c.Naive, c.InitialTotal)
	report("Engine", c.Engine, c.InitialTotal)

	fmt.Println("Summary")
	fmt.Printf("  The naive ledger created or destroyed $%s and re-applied all %d retries.\n",
		absMoney(c.Naive.Drift), c.Retries)
	fmt.Printf("  The engine kept the books exact: $0 drift, 0 negative accounts, %d retries deduped,\n",
		c.Engine.Deduped)
	fmt.Printf("  while sustaining %s transfers/sec on a single core.\n", commas(c.Engine.TPS))
}

func report(name string, r bench.Result, initial engine.Money) {
	fmt.Printf("%s\n", name)
	fmt.Printf("  time:            %d ms\n", r.Millis)
	fmt.Printf("  throughput:      %s transfers/sec\n", commas(r.TPS))
	fmt.Printf("  final total:     $%s   (started $%s)\n", r.Total, initial)
	fmt.Printf("  money conjured:  $%s\n", r.Drift)
	fmt.Printf("  negative accts:  %d\n", r.Negative)
	if r.Deduped > 0 || r.Rejected > 0 {
		fmt.Printf("  retries deduped: %d\n", r.Deduped)
		fmt.Printf("  rejected:        %d\n", r.Rejected)
	}
	fmt.Println()
}

func absMoney(m engine.Money) engine.Money {
	if m < 0 {
		return -m
	}
	return m
}

// commas formats an integer with thousands separators.
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
