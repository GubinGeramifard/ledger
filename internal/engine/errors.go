package engine

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the engine. Callers can match them with
// errors.Is to react to specific rejection reasons.
var (
	ErrAccountExists = errors.New("account already exists")
	ErrBadAmount     = errors.New("amount must be positive")
	ErrSameAccount   = errors.New("source and destination are the same account")
	ErrInsufficient  = errors.New("insufficient funds")
)

// ErrNoAccount is returned when a transfer names an account that does not exist.
func ErrNoAccount(id string) error {
	return fmt.Errorf("account %q not found", id)
}
