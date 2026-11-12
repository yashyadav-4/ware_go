CREATE TABLE IF NOT EXISTS rebalance_proposals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    analysis TEXT NOT NULL,
    proposals_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_rebalance_status ON rebalance_proposals(status, created_at DESC);