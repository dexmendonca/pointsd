// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
//
// Every Store operation that changes the ledger is atomic and idempotent on its
// own: correctness does not depend on the caller checking state first, so it
// holds when several replicas handle the same payment or member concurrently.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrDuplicate is returned by Append when an entry with the same payment and
// kind already exists (UNIQUE (payment_id, kind) in Postgres).
var ErrDuplicate = errors.New("ledger: duplicate entry")

// ErrNotAwarded is returned by Reverse when the payment has no award entry.
var ErrNotAwarded = errors.New("ledger: payment was never awarded")

type Store interface {
	// Award atomically records the award for e.PaymentID. e.Points is the
	// uncapped amount; the stored points are cut so that the member's awards in
	// e.Month never exceed monthlyCap. If the payment already has an award, it is
	// returned unchanged with created == false.
	Award(ctx context.Context, e Entry, monthlyCap int64) (stored Entry, created bool, err error)

	// Reverse atomically records the reversal of paymentID, removing exactly the
	// points its award entry holds. If the payment was already reversed, the
	// existing reversal is returned with created == false. It returns
	// ErrNotAwarded if the payment has no award.
	Reverse(ctx context.Context, paymentID string) (stored Entry, created bool, err error)

	// Append inserts a raw entry. It enforces uniqueness of (PaymentID, Kind)
	// but no earning rule; the rewards handler uses Award and Reverse.
	Append(ctx context.Context, e Entry) error

	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type entryKey struct {
	paymentID string
	kind      Kind
}

type monthKey struct {
	memberID string
	month    string
}

// MemoryStore is an in-memory Store. Each call is one simulated round trip
// followed by one critical section, the equivalent of one Postgres transaction.
// The latency is spent outside the lock, so calls for different members overlap.
type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
	byKey   map[entryKey]int   // index into entries; the UNIQUE (payment_id, kind) constraint
	awarded map[monthKey]int64 // sum of award points per member and month
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{
		byKey:   make(map[entryKey]int),
		awarded: make(map[monthKey]int64),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

// insert appends e. The caller holds s.mu and has checked uniqueness.
func (s *MemoryStore) insert(e Entry) {
	s.byKey[entryKey{e.PaymentID, e.Kind}] = len(s.entries)
	s.entries = append(s.entries, e)
	if e.Kind == KindAward {
		s.awarded[monthKey{e.MemberID, e.Month}] += e.Points
	}
}

func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if i, ok := s.byKey[entryKey{e.PaymentID, KindAward}]; ok {
		return s.entries[i], false, nil
	}
	e.Kind = KindAward
	room := max(monthlyCap-s.awarded[monthKey{e.MemberID, e.Month}], 0)
	e.Points = min(max(e.Points, 0), room)
	s.insert(e)
	return e, true, nil
}

func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if i, ok := s.byKey[entryKey{paymentID, KindReversal}]; ok {
		return s.entries[i], false, nil
	}
	i, ok := s.byKey[entryKey{paymentID, KindAward}]
	if !ok {
		return Entry{}, false, ErrNotAwarded
	}
	award := s.entries[i]
	rev := Entry{
		MemberID:  award.MemberID,
		PaymentID: award.PaymentID,
		Kind:      KindReversal,
		Points:    -award.Points,
		Month:     award.Month,
	}
	s.insert(rev)
	return rev, true, nil
}

// HasEntry reports whether an entry exists for paymentID and kind. It is a
// read-only helper and not part of Store: deciding to write based on its
// answer is a race between replicas.
func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byKey[entryKey{paymentID, kind}]
	return ok, nil
}

// AwardedInMonth returns the award points of memberID in month. It is a
// read-only helper and not part of Store (see HasEntry).
func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.awarded[monthKey{memberID, month}], nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byKey[entryKey{e.PaymentID, e.Kind}]; ok {
		return ErrDuplicate
	}
	s.insert(e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
