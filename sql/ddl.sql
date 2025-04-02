CREATE TABLE raw_stations (
    id VARCHAR,
    deviceid VARCHAR,
    name VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    PRIMARY KEY (id)
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

CREATE TABLE raw_readings (
    timestamp VARCHAR,
    stationid VARCHAR,
    value VARCHAR,
    PRIMARY KEY (timestamp, stationid)
    )
;

CREATE TABLE readings (
    timestamp timestamp,
    stationid VARCHAR,
    value NUMERIC,
    PRIMARY KEY (timestamp, stationid)
    )
;
