-- Add scheduling and ownership metadata to queued_items.
--
-- These columns allow the durable FIFO queue to act as an availability-aware
-- scheduler:
--   not_before_ms   — earliest wall-clock time the item may be dispatched
--   state           — 'pending' | 'blocked' | 'leased' | 'done'
--   block_reason    — short string describing why the item is blocked
--   attempt_count   — number of failed dispatch attempts
--   lease_owner     — worker ID that currently owns the item
--   lease_expiry_ms — when the current lease expires (0 = no lease)
--   last_error      — bounded error message from the most recent failure
--
-- All new columns are nullable or have defaults so legacy rows are treated
-- as immediately-due pending items with no lease.

ALTER TABLE queued_items ADD COLUMN not_before_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queued_items ADD COLUMN state TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE queued_items ADD COLUMN block_reason TEXT;
ALTER TABLE queued_items ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queued_items ADD COLUMN lease_owner TEXT;
ALTER TABLE queued_items ADD COLUMN lease_expiry_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queued_items ADD COLUMN last_error TEXT;

-- Index for finding globally due, unleased head items across all threads.
-- A head item is the one with the smallest queue_order for its thread.
CREATE INDEX queued_items_due_idx
    ON queued_items(state, not_before_ms, lease_expiry_ms)
    WHERE state = 'pending' OR state = 'blocked';
