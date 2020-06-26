/*

TimescaleDB schema to capture the analyzed data.
To install TimescaleDB, follow the instructions on https://docs.timescale.com/v1.3/getting-started/installation.
To deploy this

sudo psql -U postgres -d [database-name] -a -f tsdb-schema.sql

*/

DROP TABLE IF EXISTS "clean_audio";
CREATE TABLE "clean_audio"(
	song_id INTEGER,
	timeseries INTEGER,
	intensity FLOAT
);

SELECT create_hypertable('clean_audio', 'timeseries');
CREATE INDEX on clean_audio (song_id, timeseries DESC);

