package rewards_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

// flakyStore fails the first n calls of op with ledger.ErrUnavailable.
func flakyStore(op string, n int32) *ledger.MemoryStore {
	var left atomic.Int32
	left.Store(n)
	return ledger.NewMemoryStore(ledger.WithFaults(func(got string) error {
		if got == op && left.Add(-1) >= 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))
}

func TestReversal_ConcurrentRedeliveryAcrossReplicasReversesOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}
	deliver(t, replicas[0], settled("pay_1", "mbr_1", 2000))
	deliver(t, replicas[0], settled("pay_2", "mbr_1", 1500))

	rev := reversed("pay_2", "mbr_1", 1500)
	var fns []func()
	for i := 0; i < 9; i++ {
		h := replicas[i%3]
		fns = append(fns, func() { deliver(t, h, rev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000 (reversal applied more than once)", got)
	}
}

func TestAward_ConcurrentRepublishWithNewEventIDsAcrossReplicas(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}

	var fns []func()
	for i := 0; i < 9; i++ {
		h, ev := replicas[i%3], settled("pay_1", "mbr_1", 3000)
		ev.EventID = fmt.Sprintf("evt_republish_%d", i)
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 3000 {
		t.Fatalf("balance = %d, want 3000", got)
	}
}

func TestAward_ConcurrentCapAcrossThreeReplicasWithFaults(t *testing.T) {
	var calls atomic.Int32
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency), ledger.WithFaults(func(op string) error {
		if op != "balance" && calls.Add(1)%3 == 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}

	var fns []func()
	for i := 0; i < 24; i++ {
		// Every payment is delivered twice, to different replicas.
		h1, h2, ev := replicas[i%3], replicas[(i+1)%3], settled(fmt.Sprintf("pay_%d", i/2), "mbr_1", 900)
		fns = append(fns, func() { deliver(t, h1, ev) }, func() { deliver(t, h2, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != rewards.DefaultPolicy.MonthlyCap {
		t.Fatalf("balance = %d, want %d", got, rewards.DefaultPolicy.MonthlyCap)
	}
}

func TestReversal_DoesNotGiveCapRoomBack(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 10_000))
	deliver(t, h, reversed("pay_1", "mbr_1", 10_000))
	deliver(t, h, settled("pay_2", "mbr_1", 500))

	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0 (reversals do not give cap room back)", got)
	}
}

func TestReversal_OfPaymentCappedToZeroRemovesNothing(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 10_000))
	deliver(t, h, settled("pay_2", "mbr_1", 800)) // cap reached: 0 points
	deliver(t, h, reversed("pay_2", "mbr_1", 800))

	if got := balance(t, store, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}

func TestReversal_TransientLedgerErrorIsRetried(t *testing.T) {
	store := flakyStore("reverse", 2)
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 1200))

	first := newMessage(t, reversed("pay_1", "mbr_1", 1200))
	h.Handle(context.Background(), first)
	if first.outcome(t) != nacked {
		t.Fatal("reversal was acked although the ledger was unavailable")
	}
	deliver(t, h, reversed("pay_1", "mbr_1", 1200))

	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestHandle_LedgerUnavailableIsNackedNotAcked(t *testing.T) {
	store := flakyStore("award", 1)
	msg := newMessage(t, settled("pay_1", "mbr_1", 700))

	newHandler(store).Handle(context.Background(), msg)

	if msg.outcome(t) != nacked {
		t.Fatal("event was acked although the ledger write failed; it would be lost")
	}
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestHandle_CanceledContextIsNacked(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := newMessage(t, settled("pay_1", "mbr_1", 700))

	newHandler(store).Handle(ctx, msg)

	if msg.outcome(t) != nacked {
		t.Fatal("event was acked although it was not processed")
	}
}

func TestReversal_BeforeSettlementIsAckedAndWritesNothing(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, reversed("pay_1", "mbr_1", 900))
	deliver(t, h, settled("pay_1", "mbr_1", 900))

	if got := balance(t, store, "mbr_1"); got != 900 {
		t.Fatalf("balance = %d, want 900", got)
	}
}
