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
    PRIMARY KEY (timestamp, stationid)
    )
;
