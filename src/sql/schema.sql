-- Aurora DSQL schema for V16 incident history and real-time chart buckets.
-- Idempotent: safe to run multiple times.

-- 1) Incidents (one row per DGT incident / beacon-like situation)
CREATE TABLE IF NOT EXISTS incidents (
  incident_id          TEXT PRIMARY KEY,

  -- DGT identifiers
  dgt_situation_id     TEXT,
  dgt_record_id        TEXT,
  dgt_creation_ref     TEXT,

  -- Dimensions (filters)
  municipality_id      TEXT NOT NULL,
  municipality_name    TEXT NOT NULL,
  province_name        TEXT NOT NULL,

  road_name            TEXT,
  road_key             TEXT NOT NULL,
  road_type            TEXT NOT NULL,
  km                  TEXT,
  lat                 DOUBLE PRECISION,
  lon                 DOUBLE PRECISION,

  -- Lifecycle & timing
  status              TEXT NOT NULL, -- active | ended
  validity_status     TEXT,
  start_time_feed     TIMESTAMPTZ,
  end_time_feed       TIMESTAMPTZ,
  first_seen_at       TIMESTAMPTZ NOT NULL,
  last_seen_at        TIMESTAMPTZ NOT NULL,
  ended_at            TIMESTAMPTZ,
  end_reason          TEXT,
  seen_count          INTEGER NOT NULL DEFAULT 0,

  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_incidents_mun_time
  ON incidents (municipality_id, first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_incidents_mun_road_time
  ON incidents (municipality_id, road_key, first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_incidents_mun_type_time
  ON incidents (municipality_id, road_type, first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_incidents_status_last_seen
  ON incidents (status, last_seen_at DESC);

-- 2) Real-time buckets by municipality+road (one row per minute)
CREATE TABLE IF NOT EXISTS minute_buckets_road (
  municipality_id   TEXT NOT NULL,
  road_key          TEXT NOT NULL,
  bucket_minute     TIMESTAMPTZ NOT NULL,

  -- denormalized dimensions for display/filtering
  road_type         TEXT NOT NULL,
  road_name         TEXT,

  -- metrics
  active_count      INTEGER NOT NULL,
  new_count         INTEGER NOT NULL,
  ended_count       INTEGER NOT NULL,

  PRIMARY KEY (municipality_id, road_key, bucket_minute)
);

-- 3) Real-time buckets by municipality+road_type (one row per minute)
CREATE TABLE IF NOT EXISTS minute_buckets_type (
  municipality_id   TEXT NOT NULL,
  road_type         TEXT NOT NULL,
  bucket_minute     TIMESTAMPTZ NOT NULL,

  active_count      INTEGER NOT NULL,
  new_count         INTEGER NOT NULL,
  ended_count       INTEGER NOT NULL,

  PRIMARY KEY (municipality_id, road_type, bucket_minute)
);

-- 4) Real-time buckets by municipality only (one row per minute)
CREATE TABLE IF NOT EXISTS minute_buckets_mun (
  municipality_id   TEXT NOT NULL,
  bucket_minute     TIMESTAMPTZ NOT NULL,

  active_count      INTEGER NOT NULL,
  new_count         INTEGER NOT NULL,
  ended_count       INTEGER NOT NULL,

  PRIMARY KEY (municipality_id, bucket_minute)
);

