CREATE TABLE raw_stations (
    id VARCHAR,
    deviceid VARCHAR,
    name VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC,
    PRIMARY KEY(id)
    )
;

CREATE TABLE raw_readings (
    timestamp timestamp,
    stationid VARCHAR,
    value NUMERIC,
    PRIMARY KEY (timestamp, stationId)
    )
;

CREATE TABLE stations (
    id VARCHAR,
    deviceid VARCHAR,
    name VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC,
    PRIMARY KEY(id)
    )
;

CREATE TABLE readings (
    timestamp timestamp,
    stationid VARCHAR REFERENCES stations (id),
    value NUMERIC,
    PRIMARY KEY (timestamp, stationId)
    )
;
