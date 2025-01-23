-- Add migration script here
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    schedule JSONB NOT NULL,
    dependencies UUID[] NOT NULL DEFAULT '{}',
    status JSONB NOT NULL,
    time_out BIGINT NOT NULL, -- stored in milliseconds
    retry_policy JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);