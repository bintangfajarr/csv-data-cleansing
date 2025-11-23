-- DDL Script for Data Engineer Test
-- Database: PostgreSQL

DROP TABLE IF EXISTS data_reject;

DROP TABLE IF EXISTS data;

CREATE TABLE data (
    dates DATE NOT NULL,
    ids VARCHAR(255) PRIMARY KEY,
    names VARCHAR(500) NOT NULL,
    monthly_listeners INTEGER,
    popularity INTEGER,
    followers BIGINT,
    genres TEXT[],
    first_release VARCHAR(4),
    last_release VARCHAR(4),
    num_releases INTEGER,
    num_tracks INTEGER,
    playlists_found VARCHAR(50),
    feat_track_ids TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data_reject (
    dates DATE NOT NULL,
    ids VARCHAR(255),
    names VARCHAR(500) NOT NULL,
    monthly_listeners INTEGER,
    popularity INTEGER,
    followers BIGINT,
    genres TEXT[],
    first_release VARCHAR(4),
    last_release VARCHAR(4),
    num_releases INTEGER,
    num_tracks INTEGER,
    playlists_found VARCHAR(50),
    feat_track_ids TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reject_reason VARCHAR(255)
);

CREATE INDEX idx_data_dates ON data (dates);

CREATE INDEX idx_data_names ON data (names);

CREATE INDEX idx_data_reject_ids ON data_reject (ids);

COMMENT ON
TABLE data IS 'Table containing cleaned and deduplicated data';

COMMENT ON
TABLE data_reject IS 'Table containing duplicate/rejected records';

COMMENT ON COLUMN data_reject.reject_reason IS 'Reason why the record was rejected';