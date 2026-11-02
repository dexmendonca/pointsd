// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = errors.New("rewards: reversal for unknown payment")

type Handler struct {
	store      ledger.Store
	policy     Policy
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(store ledger.Store, policy Policy, deadLetter pubsub.Publisher, log *slog.Logger) *Handler {
	return &Handler{store: store, policy: policy, deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently
// (MaxOutstandingMessages per replica), and pointsd runs several replicas.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	switch err := h.process(ctx, ev); {
	case err == nil:
		msg.Ack()
	case errors.Is(err, ErrUnknownPayment):
		// Retrying cannot succeed; finance reconciles these from the logs.
		h.log.Warn("reversal for payment that never earned points",
			"payment_id", ev.PaymentID, "member_id", ev.MemberID, "amount_cents", ev.AmountCents, "event_id", ev.EventID)
		msg.Ack()
	default:
		// Transient (ledger unavailable, context done). Every ledger operation is
		// idempotent, so redelivery is safe even if the write was committed.
		h.log.Error("processing failed, will be redelivered",
			"payment_id", ev.PaymentID, "type", ev.Type, "attempt", msg.DeliveryAttempt(), "err", err)
		msg.Nack()
	}
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.award(ctx, ev)
	case events.TypeReversed:
		return h.reverse(ctx, ev)
	}
	return nil
}

// award records the payment's points. Deduplication by payment_id and the
// monthly cap are enforced atomically by the ledger, not by reading first.
func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	stored, created, err := h.store.Award(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindAward,
		Points:    h.policy.Points(ev.AmountCents),
		Month:     Month(ev.OccurredAt),
	}, h.policy.MonthlyCap)
	if err != nil {
		return err
	}
	if !created {
		h.log.Info("payment already awarded", "payment_id", ev.PaymentID, "event_id", ev.EventID, "points", stored.Points)
	}
	return nil
}

// reverse removes the points the payment's award entry holds, which may be
// less than the event amount if the award was cut by the monthly cap.
func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	stored, created, err := h.store.Reverse(ctx, ev.PaymentID)
	if errors.Is(err, ledger.ErrNotAwarded) {
		return ErrUnknownPayment
	}
	if err != nil {
		return err
	}
	if !created {
		h.log.Info("payment already reversed", "payment_id", ev.PaymentID, "event_id", ev.EventID)
		return nil
	}
	if stored.MemberID != ev.MemberID {
		h.log.Warn("reversal member differs from award member; reversed the award's member",
			"payment_id", ev.PaymentID, "event_member_id", ev.MemberID, "award_member_id", stored.MemberID)
	}
	if stored.Points == 0 {
		h.log.Warn("reversal of payment that earned 0 points (monthly cap)",
			"payment_id", ev.PaymentID, "member_id", stored.MemberID, "amount_cents", ev.AmountCents)
	}
	return nil
}
