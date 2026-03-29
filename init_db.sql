-- ============================================
-- Pump.fun SOL Lottery Bot - Database Schema
-- ============================================
-- Run this once in your Supabase SQL editor.
-- Dashboard: https://supabase.com/dashboard → SQL Editor → New Query → Paste & Run

-- ============================================
-- Pot balances (single-row, source of truth)
-- ============================================
CREATE TABLE IF NOT EXISTS lottery_pots (
    id                      INTEGER PRIMARY KEY DEFAULT 1,

    -- Accumulated SOL for each pot (in lamports, integers — no float rounding errors)
    hourly_pot_lamports     BIGINT NOT NULL DEFAULT 0,
    jackpot_pot_lamports    BIGINT NOT NULL DEFAULT 0,

    -- Total collected all time (for stats)
    total_collected_lamports BIGINT NOT NULL DEFAULT 0,

    -- Cycle counters
    hourly_draws_count      INTEGER NOT NULL DEFAULT 0,
    jackpot_draws_count     INTEGER NOT NULL DEFAULT 0,

    -- Next draw timestamps (set by bot on startup)
    next_hourly_draw_at     TIMESTAMPTZ,
    next_jackpot_draw_at    TIMESTAMPTZ,

    -- Bot heartbeat (last time bot was alive)
    last_collection_at      TIMESTAMPTZ,
    last_updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Only one row ever
    CONSTRAINT single_row CHECK (id = 1)
);

-- Seed the single row
INSERT INTO lottery_pots (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- ============================================
-- Payout lock (prevents double-pay on crash)
-- ============================================
-- Before sending a payout, we insert a pending lock row.
-- After on-chain confirmation, we mark it confirmed.
-- On bot restart, any 'pending' lock older than 5 min is checked
-- against the chain before proceeding.
CREATE TABLE IF NOT EXISTS payout_locks (
    id              BIGSERIAL PRIMARY KEY,
    draw_type       TEXT NOT NULL CHECK (draw_type IN ('hourly', 'jackpot')),
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'failed')),
    amount_lamports BIGINT NOT NULL,
    winner_wallet   TEXT NOT NULL,
    signature       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at    TIMESTAMPTZ
);

-- ============================================
-- Winners history
-- ============================================
CREATE TABLE IF NOT EXISTS lottery_winners (
    id              BIGSERIAL PRIMARY KEY,
    draw_type       TEXT NOT NULL CHECK (draw_type IN ('hourly', 'jackpot')),
    winner_wallet   TEXT NOT NULL,
    amount_lamports BIGINT NOT NULL,
    amount_sol      DOUBLE PRECISION NOT NULL,
    amount_usd      DOUBLE PRECISION,          -- snapshot at time of draw
    sol_price_usd   DOUBLE PRECISION,          -- SOL price used for conversion
    signature       TEXT NOT NULL,
    draw_number     INTEGER NOT NULL,          -- which draw number this was
    drawn_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast frontend queries
CREATE INDEX IF NOT EXISTS idx_winners_draw_type ON lottery_winners(draw_type, drawn_at DESC);
CREATE INDEX IF NOT EXISTS idx_winners_drawn_at ON lottery_winners(drawn_at DESC);

-- ============================================
-- Fee collection log (audit trail)
-- ============================================
CREATE TABLE IF NOT EXISTS collection_log (
    id                      BIGSERIAL PRIMARY KEY,
    collected_lamports      BIGINT NOT NULL,
    hourly_share_lamports   BIGINT NOT NULL,
    jackpot_share_lamports  BIGINT NOT NULL,
    hourly_pot_after        BIGINT NOT NULL,
    jackpot_pot_after       BIGINT NOT NULL,
    collected_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
