CREATE TABLE IF NOT EXISTS swarm_runs (
    run_id uuid PRIMARY KEY, scope text NOT NULL, admission_key text NOT NULL,
    fingerprint text NOT NULL, goal text NOT NULL, config jsonb NOT NULL,
    coordinator uuid, status text NOT NULL DEFAULT 'active', reason text,
    deadline timestamptz NOT NULL, step integer NOT NULL DEFAULT 0,
    sequence bigint NOT NULL DEFAULT 0, owner uuid, lease timestamptz,
    fence bigint NOT NULL DEFAULT 0, final_answer text,
    UNIQUE (scope, admission_key)
);
CREATE TABLE IF NOT EXISTS swarm_roster (
    run_id uuid REFERENCES swarm_runs, peer_id uuid, name text NOT NULL,
    template text NOT NULL, prompt text NOT NULL, revision integer NOT NULL DEFAULT 1,
    creator uuid, ttl_turns integer CHECK (ttl_turns > 0),
    turns_used integer NOT NULL DEFAULT 0 CHECK (turns_used >= 0),
    last_active_step integer NOT NULL DEFAULT 0, retirement text,
    PRIMARY KEY (run_id, peer_id),
    FOREIGN KEY (run_id, creator) REFERENCES swarm_roster
);
CREATE UNIQUE INDEX IF NOT EXISTS swarm_active_names ON swarm_roster (run_id, name) WHERE retirement IS NULL;
ALTER TABLE swarm_roster ADD COLUMN IF NOT EXISTS allowed_tools text[];
CREATE TABLE IF NOT EXISTS swarm_mailbox (
    run_id uuid, message_id uuid, sender uuid, recipient uuid NOT NULL,
    kind text NOT NULL, correlation_id uuid NOT NULL, in_reply_to uuid,
    payload jsonb NOT NULL, queue_sequence bigint NOT NULL,
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','in_flight','acknowledged','cancelled')),
    activation uuid, attempt integer NOT NULL DEFAULT 0, fence bigint, pinned jsonb,
    PRIMARY KEY (run_id, message_id),
    FOREIGN KEY (run_id, sender) REFERENCES swarm_roster,
    FOREIGN KEY (run_id, recipient) REFERENCES swarm_roster,
    FOREIGN KEY (run_id, in_reply_to) REFERENCES swarm_mailbox
);
CREATE INDEX IF NOT EXISTS swarm_queue ON swarm_mailbox (run_id, queue_sequence) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS swarm_recipient ON swarm_mailbox (run_id, recipient, state);
CREATE INDEX IF NOT EXISTS swarm_correlation ON swarm_mailbox (run_id, correlation_id);
CREATE TABLE IF NOT EXISTS swarm_artifacts (
    run_id uuid, artifact_key text, version integer, producer uuid NOT NULL,
    activation uuid NOT NULL, media_type text NOT NULL DEFAULT 'application/json',
    content jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, artifact_key, version),
    FOREIGN KEY (run_id, producer) REFERENCES swarm_roster
);
CREATE TABLE IF NOT EXISTS swarm_ledger (
    run_id uuid REFERENCES swarm_runs, sequence bigint, kind text NOT NULL,
    activation uuid, actor uuid, operation text, payload jsonb NOT NULL,
    schema_version integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, sequence), UNIQUE (run_id, operation)
);
CREATE TABLE IF NOT EXISTS swarm_response_markers (
    model text NOT NULL, endpoint_key text NOT NULL, marker text NOT NULL,
    learned_run uuid NOT NULL, rejection_sequence bigint NOT NULL,
    PRIMARY KEY (model, endpoint_key, marker),
    FOREIGN KEY (learned_run, rejection_sequence) REFERENCES swarm_ledger (run_id, sequence)
);
