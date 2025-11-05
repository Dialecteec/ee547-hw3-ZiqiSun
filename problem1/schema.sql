-- problem1/schema.sql
CREATE TABLE IF NOT EXISTS lines (
  line_id      SERIAL PRIMARY KEY,
  line_name    VARCHAR(64) NOT NULL UNIQUE,
  vehicle_type VARCHAR(16) NOT NULL CHECK (vehicle_type IN ('rail','bus'))
);
CREATE TABLE IF NOT EXISTS stops (
  stop_id   SERIAL PRIMARY KEY,
  stop_name VARCHAR(128) NOT NULL UNIQUE,
  latitude  DOUBLE PRECISION,
  longitude DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS line_stops (
  line_stop_id SERIAL PRIMARY KEY,
  line_id      INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
  stop_id      INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
  sequence     INTEGER NOT NULL CHECK (sequence > 0),
  time_offset_minutes INTEGER NOT NULL CHECK (time_offset_minutes >= 0),
  UNIQUE (line_id, sequence)
);
CREATE TABLE IF NOT EXISTS trips (
  trip_id             VARCHAR(32) PRIMARY KEY,
  line_id             INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
  scheduled_departure TIMESTAMP NOT NULL,
  vehicle_id          VARCHAR(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS stop_events (
  stop_event_id SERIAL PRIMARY KEY,
  trip_id       VARCHAR(32) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
  stop_id       INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE RESTRICT,
  scheduled     TIMESTAMP NOT NULL,
  actual        TIMESTAMP NOT NULL,
  passengers_on  INTEGER NOT NULL CHECK (passengers_on  >= 0),
  passengers_off INTEGER NOT NULL CHECK (passengers_off >= 0)
);
CREATE INDEX IF NOT EXISTS idx_trips_line ON trips(line_id);
CREATE INDEX IF NOT EXISTS idx_events_trip ON stop_events(trip_id);
CREATE INDEX IF NOT EXISTS idx_events_stop ON stop_events(stop_id);
