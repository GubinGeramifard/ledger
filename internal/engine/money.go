package engine

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Money is an amount in minor units (cents). Financial systems never store
// money in floating point: 0.1 + 0.2 != 0.3 in float64, and those tiny errors
// accumulate into real discrepancies. An int64 of cents is exact and holds
// values up to about 92 quadrillion dollars, which is plenty.
type Money int64

// String renders the amount as a decimal, e.g. Money(12345) -> "123.45".
func (m Money) String() string {
	neg := m < 0
	v := int64(m)
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%d.%02d", v/100, v%100)
	if neg {
		return "-" + s
	}
	return s
}

// ParseMoney parses a decimal string like "123.45" into minor units. It accepts
// zero, one, or two decimal places and rejects anything finer than a cent.
func ParseMoney(s string) (Money, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty amount")
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	whole, frac, hasFrac := strings.Cut(s, ".")
	cents, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", s)
	}
	cents *= 100
	if hasFrac {
		if len(frac) > 2 {
			return 0, fmt.Errorf("amount %q is finer than one cent", s)
		}
		frac = (frac + "00")[:2]
		f, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid amount %q", s)
		}
		cents += f
	}
	if neg {
		cents = -cents
	}
	return Money(cents), nil
}
