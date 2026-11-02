# Ledger in Postgres

Tables: `ledger_entries(member_id, payment_id, kind, points, month, UNIQUE (payment_id, kind))` and
`member_month_awards(member_id, month, awarded, PRIMARY KEY (member_id, month))`.

**Award** (one transaction, READ COMMITTED):
1. `INSERT INTO member_month_awards VALUES ($member, $month, 0) ON CONFLICT DO NOTHING;`
2. `SELECT awarded FROM member_month_awards WHERE member_id=$member AND month=$month FOR UPDATE;`
3. `SELECT points FROM ledger_entries WHERE payment_id=$pay AND kind='award';` if found: COMMIT, return it.
4. `pts := LEAST($points, GREATEST($cap - awarded, 0))`; INSERT the award row with pts; `UPDATE ... SET awarded = awarded + pts`; COMMIT.

**Reverse** (one statement): `INSERT INTO ledger_entries SELECT member_id, payment_id, 'reversal', -points, month
FROM ledger_entries WHERE payment_id=$pay AND kind='award' ON CONFLICT (payment_id, kind) DO NOTHING;`
No award row means the payment never earned: ack + log.

Why it holds across replicas: no write is decided from a read outside its own transaction. The row lock in step 2
serializes only the same member+month (other members stay parallel), so the cap sees committed totals.
UNIQUE (payment_id, kind) makes a payment earn and reverse at most once, even if replicas race (the loser gets 23505,
rolls back, is nacked, and the retry sees the row). Reversals copy the stored points, never the event amount.
Any error means nack; a retry after a commit whose reply was lost is a no-op thanks to the unique key.
