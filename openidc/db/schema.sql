-- OpenIDC database schema.
--
-- This is the CSS 3.0 (Center for Seismic Studies, version 3.0) relational
-- schema that the CTBTO IDC uses for waveform processing results. Column names
-- and semantics follow the published CSS 3.0 specification so the schema is
-- recognisable to anyone who has worked with IDC / NDC-in-a-Box data.
--
-- Phase 1 of OpenIDC populates `wfdisc` (waveform descriptors) and `arrival`
-- (automatic detections). `origin`, `assoc` and `event` are created here so the
-- Phase 2 network-association / event-screening stage can populate them.

-- Sequences for the CSS surrogate ids (wfid, arid, orid, evid). The IDC issues
-- these from id servers; sequences are the Postgres-native equivalent.
CREATE SEQUENCE IF NOT EXISTS wfid_seq START 1;
CREATE SEQUENCE IF NOT EXISTS arid_seq START 1;
CREATE SEQUENCE IF NOT EXISTS orid_seq START 1;
CREATE SEQUENCE IF NOT EXISTS evid_seq START 1;

-- ---------------------------------------------------------------------------
-- wfdisc : one row describes one waveform segment (the data the detector saw).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wfdisc (
    sta        VARCHAR(6)   NOT NULL,            -- station code
    chan       VARCHAR(8)   NOT NULL,            -- channel code
    time       DOUBLE PRECISION NOT NULL,        -- epoch time of first sample
    wfid       BIGINT       PRIMARY KEY,         -- waveform id
    chanid     BIGINT,
    jdate      INTEGER,                          -- julian date yyyyddd
    endtime    DOUBLE PRECISION,
    nsamp      INTEGER,
    samprate   REAL,                             -- samples per second
    calib      REAL    DEFAULT 1.0,
    calper     REAL    DEFAULT -1.0,
    instype    VARCHAR(6),
    segtype    VARCHAR(1) DEFAULT 'o',
    datatype   VARCHAR(2) DEFAULT 's4',
    dir        VARCHAR(64),
    dfile      VARCHAR(32),
    foff       INTEGER DEFAULT 0,
    lddate     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS wfdisc_sta_time ON wfdisc (sta, time);

-- ---------------------------------------------------------------------------
-- arrival : one row per detected signal onset (a "pick"). Written by the
--           automatic station-processing stage.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arrival (
    sta        VARCHAR(6)   NOT NULL,            -- station code
    time       DOUBLE PRECISION NOT NULL,        -- epoch time of the onset
    arid       BIGINT       PRIMARY KEY,         -- arrival id
    jdate      INTEGER,
    chanid     BIGINT,
    chan       VARCHAR(8),
    iphase     VARCHAR(8)  DEFAULT 'P',          -- reported phase
    stype      VARCHAR(1),
    deltim     REAL    DEFAULT -1.0,             -- onset time uncertainty (s)
    azimuth    REAL    DEFAULT -1.0,
    slow       REAL    DEFAULT -1.0,
    amp        REAL    DEFAULT -1.0,             -- measured amplitude
    per        REAL    DEFAULT -1.0,             -- measured period
    snr        REAL    DEFAULT -1.0,             -- signal-to-noise (STA/LTA ratio)
    qual       VARCHAR(1),
    auth       VARCHAR(20) DEFAULT 'OpenIDC',    -- producer of this row
    commid     BIGINT,
    lddate     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS arrival_sta_time ON arrival (sta, time);

-- ---------------------------------------------------------------------------
-- origin : a located event hypothesis (Phase 2 network processing output).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS origin (
    lat        REAL,
    lon        REAL,
    depth      REAL,
    time       DOUBLE PRECISION,
    orid       BIGINT  PRIMARY KEY,
    evid       BIGINT,
    jdate      INTEGER,
    nass       INTEGER DEFAULT 0,                -- number of associated phases
    ndef       INTEGER DEFAULT 0,
    etype      VARCHAR(7),                       -- event type (e.g. 'qb','ke')
    mb         REAL    DEFAULT -999.0,           -- body-wave magnitude
    ms         REAL    DEFAULT -999.0,           -- surface-wave magnitude
    algorithm  VARCHAR(15),
    auth       VARCHAR(20) DEFAULT 'OpenIDC',
    lddate     TIMESTAMPTZ DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- assoc : links an arrival to an origin (which detection belongs to which event)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assoc (
    arid       BIGINT  NOT NULL,
    orid       BIGINT  NOT NULL,
    sta        VARCHAR(6),
    phase      VARCHAR(8),
    belief     REAL    DEFAULT 0.0,
    delta      REAL    DEFAULT -1.0,             -- station-event distance (deg)
    timeres    REAL    DEFAULT -999.0,
    timedef    VARCHAR(1) DEFAULT 'd',
    wgt        REAL    DEFAULT -1.0,
    auth       VARCHAR(20) DEFAULT 'OpenIDC',
    lddate     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (arid, orid)
);

-- ---------------------------------------------------------------------------
-- event : groups origins under a single event (with the preferred origin).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event (
    evid       BIGINT  PRIMARY KEY,
    evname     VARCHAR(32),
    prefor     BIGINT,                           -- preferred origin id
    auth       VARCHAR(20) DEFAULT 'OpenIDC',
    lddate     TIMESTAMPTZ DEFAULT now()
);

-- A convenience view for the demo: most recent detections, newest first.
CREATE OR REPLACE VIEW recent_arrivals AS
    SELECT arid, sta, chan, iphase,
           to_timestamp(time) AS onset_utc,
           round(snr::numeric, 2) AS snr,
           auth
    FROM arrival
    ORDER BY time DESC;
