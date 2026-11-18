CREATE TABLE IF NOT EXISTS identities (
  id          text PRIMARY KEY,
  kind        text NOT NULL,
  source      text NOT NULL,
  version     bigint NOT NULL,
  deleted     boolean NOT NULL DEFAULT false,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entitlements (
  identity_id text NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  resource    text NOT NULL,
  permission  text NOT NULL,
  PRIMARY KEY (identity_id, resource, permission)
);

-- One row per event ever applied or skipped: makes redelivery a no-op.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     text PRIMARY KEY,
  outcome      text NOT NULL,   -- applied, stale
  processed_at timestamptz NOT NULL DEFAULT now()
);
