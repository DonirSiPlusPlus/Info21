DROP TABLE IF EXISTS time_tracking;
DROP TABLE IF EXISTS recommendations;
DROP TABLE IF EXISTS friends;
DROP TABLE IF EXISTS transferred_points;
DROP TABLE IF EXISTS verter;
DROP TABLE IF EXISTS p2p;
DROP TABLE IF EXISTS xp;
DROP TABLE IF EXISTS checks;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS peers;
DROP TYPE IF EXISTS status_check;

CREATE TYPE status_check AS ENUM ('start', 'success', 'failure');

CREATE TABLE peers (
  nickname VARCHAR PRIMARY KEY,
  birthday DATE
);

COPY peers FROM '/Users/birdpers/sql2/src/imports/import_peers.csv'
DELIMITER ',' CSV header;


CREATE TABLE tasks (
  title VARCHAR PRIMARY KEY,
  parent_task varchar REFERENCES tasks(title),
  max_XP NUMERIC NOT NULL
);

ALTER TABLE tasks add constraint ch_max_xp check (max_XP > 0);

COPY tasks FROM '/Users/birdpers/sql2/src/imports/import_tasks.csv'
DELIMITER ',' CSV header;


CREATE TABLE checks (
  id BIGINT PRIMARY KEY,
  peer VARCHAR NOT NULL REFERENCES peers(nickname),
  task VARCHAR NOT NULL REFERENCES tasks(title),
  check_date DATE NOT NULL DEFAULT current_date
);

COPY checks FROM '/Users/birdpers/sql2/src/imports/import_checks.csv'
DELIMITER ',' CSV header;



CREATE TABLE p2p (
  id BIGINT PRIMARY KEY,
  check_id BIGINT NOT NULL REFERENCES checks(id),
  checking_peer VARCHAR NOT NULL REFERENCES peers(nickname),
  state status_check DEFAULT 'start' NOT NULL,
  check_time TIME NOT NULL,
  UNIQUE (check_id, checking_peer, state)
);

COPY p2p FROM '/Users/birdpers/sql2/src/imports/import_p2p.csv'
DELIMITER ',' CSV header;


CREATE TABLE verter (
  id BIGINT PRIMARY KEY,
  check_id BIGINT NOT NULL REFERENCES checks(id),
  state status_check DEFAULT 'start' NOT NULL,
  time TIME NOT NULL,
  UNIQUE (check_id, state, time)
);

COPY verter FROM '/Users/birdpers/sql2/src/imports/import_verter.csv'
DELIMITER ',' CSV header;


CREATE TABLE xp (
  id BIGINT PRIMARY KEY,
  check_id BIGINT NOT NULL REFERENCES checks(id),
  xp_amount NUMERIC DEFAULT 0 NOT NULL
);

ALTER TABLE xp add constraint ch_xp_amount check ( xp_amount >= 0);

COPY xp FROM '/Users/birdpers/sql2/src/imports/import_xp.csv'
DELIMITER ',' CSV header;


CREATE TABLE transferred_points (
  id BIGINT PRIMARY KEY,
  checking_peer VARCHAR NOT NULL REFERENCES peers(nickname),
  checked_peer VARCHAR NOT NULL REFERENCES peers(nickname),
  points_amount NUMERIC NOT NULL DEFAULT 0
);

ALTER TABLE transferred_points ADD CONSTRAINT ch_peers check (checking_peer <> checked_peer);

COPY transferred_points FROM '/Users/birdpers/sql2/src/imports/import_transf_points.csv'
DELIMITER ',' CSV header;


CREATE TABLE friends (
  id BIGINT PRIMARY KEY,
  peer1 VARCHAR NOT NULL REFERENCES peers(nickname),
  peer2 VARCHAR NOT NULL REFERENCES peers(nickname)
);

ALTER TABLE friends ADD CONSTRAINT ch_peers check (peer1 <> peer2);

COPY friends FROM '/Users/birdpers/sql2/src/imports/import_friends.csv'
DELIMITER ',' CSV header;


CREATE TABLE recommendations (
  id BIGINT PRIMARY KEY,
  peer VARCHAR NOT NULL REFERENCES peers(nickname),
  recommended_peer VARCHAR NOT NULL REFERENCES peers(nickname)
);

ALTER TABLE recommendations ADD CONSTRAINT ch_peers check (peer <> recommended_peer);

COPY recommendations FROM '/Users/birdpers/sql2/src/imports/import_recommendations.csv'
DELIMITER ',' CSV header;


CREATE TABLE time_tracking (
  id BIGINT PRIMARY KEY,
  peer VARCHAR NOT NULL REFERENCES peers(nickname),
  date DATE DEFAULT current_date NOT NULL,
  time TIME NOT NULL,
  state numeric DEFAULT 1 NOT NULL
);

ALTER TABLE time_tracking ADD constraint ch_state check (state in (1, 2));

COPY time_tracking FROM '/Users/birdpers/sql2/src/imports/import_timetrack.csv'
DELIMITER ',' CSV header;



COPY checks TO '/Users/birdpers/sql2/src/exports/export_checks.csv'
DELIMITER ',' CSV header;

COPY peers TO '/Users/birdpers/sql2/src/exports/export_peers.csv'
DELIMITER ',' CSV header;

COPY xp TO '/Users/birdpers/sql2/src/exports/export_xp.csv'
DELIMITER ',' CSV header;

COPY tasks TO '/Users/birdpers/sql2/src/exports/export_tasks.csv'
DELIMITER ',' CSV header;

COPY verter TO '/Users/birdpers/sql2/src/exports/export_verter.csv'
DELIMITER ',' CSV header;

COPY p2p TO '/Users/birdpers/sql2/src/exports/export_p2p.csv'
DELIMITER ',' CSV header;

COPY transferred_points TO '/Users/birdpers/sql2/src/exports/export_transf_points.csv'
DELIMITER ',' CSV header;

COPY friends TO '/Users/birdpers/sql2/src/exports/export_friends.csv'
DELIMITER ',' CSV header;

COPY recommendations TO '/Users/birdpers/sql2/src/exports/export_recommendations.csv'
DELIMITER ',' CSV header;

COPY time_tracking TO '/Users/birdpers/sql2/src/exports/export_timetrack.csv'
DELIMITER ',' CSV header;
