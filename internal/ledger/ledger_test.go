package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
)

var ctx = context.Background()

func award(paymentID, memberID string, points int64) ledger.Entry {
	return ledger.Entry{MemberID: memberID, PaymentID: paymentID, Kind: ledger.KindAward, Points: points, Month: "2026-10"}
}

func parallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(i)
		}()
	}
	wg.Wait()
}

func mustAward(t *testing.T, s *ledger.MemoryStore, e ledger.Entry) ledger.Entry {
	t.Helper()
	stored, _, err := s.Award(ctx, e, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func mustBalance(t *testing.T, s *ledger.MemoryStore, member string, want int64) {
	t.Helper()
	got, err := s.Balance(ctx, member)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s balance = %d, want %d", member, got, want)
	}
}

func TestAward_ConcurrentSamePaymentIsStoredOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	var mu sync.Mutex
	created := 0
	parallel(20, func(int) {
		e, ok, err := s.Award(ctx, award("pay_1", "mbr_1", 2000), 10_000)
		if err != nil {
			t.Error(err)
			return
		}
		if e.Points != 2000 {
			t.Errorf("stored points = %d, want 2000", e.Points)
		}
		if ok {
			mu.Lock()
			created++
			mu.Unlock()
		}
	})
	if created != 1 {
		t.Fatalf("created = %d, want exactly 1", created)
	}
	mustBalance(t, s, "mbr_1", 2000)
}

func TestAward_DuplicateKeepsFirstEntry(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Award(ctx, award("pay_1", "mbr_1", 1000), 10_000); err != nil {
		t.Fatal(err)
	}
	// A republish with a different amount must not change the stored award.
	e, created, err := s.Award(ctx, award("pay_1", "mbr_1", 5000), 10_000)
	if err != nil || created || e.Points != 1000 {
		t.Fatalf("got (%+v, %v, %v), want existing 1000-point award", e, created, err)
	}
	mustBalance(t, s, "mbr_1", 1000)
}

func TestAward_ConcurrentPaymentsNeverExceedCap(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	parallel(30, func(i int) {
		if _, _, err := s.Award(ctx, award(fmt.Sprintf("pay_%d", i), "mbr_1", 700), 10_000); err != nil {
			t.Error(err)
		}
	})
	mustBalance(t, s, "mbr_1", 10_000)
}

func TestAward_CapIsPerMemberAndMonth(t *testing.T) {
	s := ledger.NewMemoryStore()
	sep := award("pay_1", "mbr_1", 10_000)
	sep.Month = "2026-09"
	for _, e := range []ledger.Entry{sep, award("pay_2", "mbr_1", 10_000), award("pay_3", "mbr_2", 10_000)} {
		if _, _, err := s.Award(ctx, e, 10_000); err != nil {
			t.Fatal(err)
		}
	}
	e, _, err := s.Award(ctx, award("pay_4", "mbr_1", 50), 10_000)
	if err != nil || e.Points != 0 {
		t.Fatalf("got (%+v, %v), want a 0-point award once the cap is used", e, err)
	}
	mustBalance(t, s, "mbr_1", 20_000)
	mustBalance(t, s, "mbr_2", 10_000)
}

func TestReverse_RemovesStoredPointsNotRequested(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 9500))
	capped := mustAward(t, s, award("pay_2", "mbr_1", 1000))
	if capped.Points != 500 {
		t.Fatalf("capped award = %d, want 500", capped.Points)
	}
	rev, created, err := s.Reverse(ctx, "pay_2")
	if err != nil || !created || rev.Points != -500 || rev.Kind != ledger.KindReversal || rev.Month != "2026-10" {
		t.Fatalf("got (%+v, %v, %v), want a -500 reversal in 2026-10", rev, created, err)
	}
	mustBalance(t, s, "mbr_1", 9500)
}

func TestReverse_DoesNotGiveCapRoomBack(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", 10_000))
	if _, _, err := s.Reverse(ctx, "pay_1"); err != nil {
		t.Fatal(err)
	}
	e, _, err := s.Award(ctx, award("pay_2", "mbr_1", 100), 10_000)
	if err != nil || e.Points != 0 {
		t.Fatalf("got (%+v, %v), want 0 points after reversal", e, err)
	}
}

func TestReverse_ConcurrentIsStoredOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	mustAward(t, s, award("pay_1", "mbr_1", 3000))
	parallel(20, func(int) {
		if _, _, err := s.Reverse(ctx, "pay_1"); err != nil {
			t.Error(err)
		}
	})
	mustBalance(t, s, "mbr_1", 0)
}

func TestReverse_UnknownPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Reverse(ctx, "pay_x"); !errors.Is(err, ledger.ErrNotAwarded) {
		t.Fatalf("err = %v, want ErrNotAwarded", err)
	}
	if ok, _ := s.HasEntry(ctx, "pay_x", ledger.KindReversal); ok {
		t.Fatal("a reversal entry was written for an unknown payment")
	}
}

func TestAppend_RejectsDuplicatePaymentAndKind(t *testing.T) {
	s := ledger.NewMemoryStore()
	if err := s.Append(ctx, award("pay_1", "mbr_1", 100)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, award("pay_1", "mbr_1", 100)); !errors.Is(err, ledger.ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
	if got, _ := s.AwardedInMonth(ctx, "mbr_1", "2026-10"); got != 100 {
		t.Fatalf("AwardedInMonth = %d, want 100", got)
	}
	mustBalance(t, s, "mbr_1", 100)
}

func TestFaults_FailedCallHasNoEffect(t *testing.T) {
	failing := true
	s := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if failing && op != "balance" {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	if _, _, err := s.Award(ctx, award("pay_1", "mbr_1", 100), 10_000); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("Award err = %v, want ErrUnavailable", err)
	}
	mustBalance(t, s, "mbr_1", 0)

	failing = false
	mustAward(t, s, award("pay_1", "mbr_1", 100))
	failing = true
	if _, _, err := s.Reverse(ctx, "pay_1"); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("Reverse err = %v, want ErrUnavailable", err)
	}
	mustBalance(t, s, "mbr_1", 100)
}
