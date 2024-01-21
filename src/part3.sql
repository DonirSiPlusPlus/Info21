-----------------------3.1
CREATE OR REPLACE FUNCTION transferred_points_readable()
RETURNS TABLE (peer1 VARCHAR, peer2 VARCHAR, points_amount NUMERIC)
AS $$
  SELECT peer1, peer2, 
    COALESCE(t1.points_amount, 0) - COALESCE(t2.points_amount, 0) AS points_amount
  FROM (
    SELECT DISTINCT
      CASE 
        WHEN checking_peer > checked_peer THEN checking_peer
        ELSE checked_peer
      END AS peer1,
      CASE
        WHEN checking_peer <= checked_peer THEN checking_peer
        ELSE checked_peer 
      END AS peer2
    FROM transferred_points
  ) AS pv 
  LEFT JOIN transferred_points t1
    ON pv.peer1 = t1.checking_peer
      AND pv.peer2 = t1.checked_peer
  LEFT JOIN transferred_points t2 
    ON pv.peer1 = t2.checked_peer
      AND pv.peer2 = t2.checking_peer
  ORDER BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM transferred_points_readable();



-----------------------3.2
CREATE OR REPLACE FUNCTION success_experience()
RETURNS TABLE (peer VARCHAR, task VARCHAR, xp NUMERIC)
AS $$
  SELECT checks.peer, checks.task, xp.xp_amount
  FROM checks
    JOIN xp
      ON xp.check_id = checks.id
    JOIN p2p
      ON p2p.check_id = checks.id
    LEFT JOIN verter
      ON verter.check_id = checks.id
  WHERE p2p.state = 'success'
    AND (verter.state = 'success' OR verter.state ISNULL);
$$ LANGUAGE SQL;
SELECT *
FROM success_experience();



-----------------------3.3
CREATE OR REPLACE FUNCTION peers_exit(date_to_check DATE)
RETURNS TABLE (peer VARCHAR)
AS $$
  (
    SELECT nickname 
    FROM peers
    EXCEPT
    SELECT DISTINCT time_tracking.peer
    FROM time_tracking
    WHERE time_tracking.date = date_to_check
  ) UNION (
    SELECT DISTINCT time_tracking.peer
    FROM time_tracking
    WHERE time_tracking.date = date_to_check
      AND state = 1
    EXCEPT
    SELECT DISTINCT time_tracking.peer
    FROM time_tracking
    WHERE time_tracking.date = date_to_check
      AND state = 2
    ORDER BY 1
  );
$$ LANGUAGE SQL;
SELECT *
FROM peers_exit('2023-06-04');



-----------------------3.4
CREATE OR REPLACE FUNCTION points_change()
RETURNS TABLE (peer VARCHAR, points_change numeric)
AS $$
  SELECT DISTINCT peers.nickname,
    COALESCE (( 
      SELECT SUM(points_amount)
        FROM transferred_points 
        WHERE checking_peer = peers.nickname
    ), 0) - COALESCE ((
      SELECT SUM(points_amount)
        FROM transferred_points
        WHERE checked_peer = peers.nickname
    ), 0) AS Change_points
  FROM peers
    LEFT JOIN transferred_points
      ON transferred_points.checking_peer = peers.nickname
  ORDER BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM points_change();



-----------------------3.5
CREATE OR REPLACE FUNCTION task5()
RETURNS TABLE (peer1 VARCHAR, Change_points NUMERIC)
AS $$
  WITH recv AS (
    SELECT peer1, SUM(points_amount) AS sum_
    FROM transferred_points_readable()
    GROUP BY peer1
  ), 
  get_ AS (
    SELECT peer2, - SUM(points_amount) AS sum_
    FROM transferred_points_readable()
    GROUP BY peer2
  ),
  chang_ AS (
    SELECT *
    FROM recv
    UNION
    SELECT *
    FROM get_
  )
  SELECT peer1 AS peer, sum(sum_) AS points_change
  FROM chang_
  GROUP BY peer1
  ORDER BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM task5();



-----------------------3.6
CREATE OR REPLACE FUNCTION most_checked_tasks()
RETURNS TABLE (day_ DATE, task VARCHAR)
AS $$
  WITH pv AS (
    SELECT check_date, task, COUNT(task) AS count_
    FROM checks
    GROUP BY 1, 2
  )
  SELECT pv.check_date, task
  FROM pv 
    JOIN (
      SELECT check_date, MAX(count_) AS max_
      FROM pv
	    GROUP BY check_date
    ) AS ps
      ON pv.check_date = ps.check_date
        AND pv.count_ = ps.max_
  ORDER BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM most_checked_tasks();



-----------------------3.7
CREATE OR REPLACE FUNCTION finish_block(block_ VARCHAR)
RETURNS TABLE (peer VARCHAR, day_ DATE)
AS $$
  WITH pv AS (
    SELECT checks.peer, check_date, COUNT(checks.task) OVER (PARTITION BY peer) AS cnt 
    FROM checks
      JOIN p2p
        ON p2p.check_id = checks.id 
      LEFT JOIN verter 
        ON checks.id = verter.check_id
    WHERE p2p.state = 'success' 
      AND (verter.state = 'success' OR verter.check_id ISNULL)
      AND checks.task IN (
	      SELECT title
        FROM tasks
        WHERE CASE
          WHEN block_ = 'C' THEN title ~* '^C[1-8]_'
          WHEN block_ = 'DO' THEN title ~* '^DO[1-6]_'
        END
    )
  )
  SELECT peer, MAX(check_date)
  FROM pv
  WHERE CASE 
    WHEN block_ = 'C' THEN cnt = 7
    WHEN block_ = 'DO' THEN cnt = 6
  END
  GROUP BY peer;
$$ LANGUAGE SQL;
SELECT *
FROM finish_block('C');



-----------------------3.8
CREATE OR REPLACE FUNCTION RecommendedPeers()
RETURNS TABLE (peer VARCHAR, recommended_peer VARCHAR)
AS $$
  WITH frs AS (
    SELECT peer1 AS peer, peer2 AS friend
    FROM friends
    UNION
    SELECT peer2 AS peer, peer1 AS friend
    FROM friends
  ), rec AS (
    SELECT frs.peer, r.recommended_peer AS rcmnd, count(recommended_peer) AS cnt
    FROM frs JOIN recommendations r ON r.peer = frs.friend AND
      frs.peer != r.recommended_peer
    GROUP BY frs.peer, rcmnd
  ), pv AS (
    SELECT peer, max(cnt) AS mx FROM rec
    GROUP BY 1
  ), lmt AS (
    SELECT pv.peer, rec.rcmnd,
      ROW_NUMBER() OVER (PARTITION BY pv.peer ORDER BY pv.peer) AS numb
    FROM pv
      LEFT JOIN rec
        ON rec.peer = pv.peer
    GROUP BY 1, 2, rec.cnt, pv.mx
    HAVING cnt = pv.mx
    ORDER BY 1
  )
  SELECT peer, rcmnd
  FROM lmt
  WHERE numb = 1
  ORDER BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM RecommendedPeers();



-----------------------3.9
CREATE OR REPLACE FUNCTION statistic(block1 VARCHAR, block2 VARCHAR)
RETURNS TABLE (started_block1 NUMERIC, started_block2 NUMERIC, started_both NUMERIC, didnt_start_any NUMERIC)
AS $$
  WITH b1 AS (
    SELECT DISTINCT peer, 1 AS cnt
    FROM checks
    WHERE CASE
      WHEN block1 = 'C' THEN task ~* '^C2_'
      WHEN block1 = 'DO' THEN task ~* '^DO1_'
    END
  ), b2 as (
    SELECT DISTINCT peer, 1 AS cnt
    FROM checks
    WHERE CASE
      WHEN block2 = 'DO' THEN task ~* '^DO1_'
      WHEN block2 = 'C' THEN task ~* '^C2_'
    END
  ), bth AS (
    SELECT b1.peer, 1 as cnt
    FROM b1
      JOIN b2
        ON b2.peer = b1.peer
  ), any_ AS (
    SELECT nickname, 1 as cnt
    FROM peers
    EXCEPT
    SELECT checks.peer, 1 as cnt
    FROM checks
    GROUP BY checks.peer
  )
  SELECT SUM(b1.cnt)*100/(SELECT COUNT(*) FROM peers),
    SUM(b2.cnt)*100/(SELECT COUNT(*) FROM peers),
    SUM(bth.cnt)*100/(SELECT COUNT(*) FROM peers),
    SUM(any_.cnt)*100/(SELECT COUNT(*) FROM peers)
  FROM b1
    FULL JOIN b2
      ON b2.peer = b1.peer
    LEFT JOIN bth
      ON bth.peer = b1.peer
    FULL JOIN any_
      ON any_.nickname = b1.peer;
$$ LANGUAGE SQL;
SELECT *
FROM statistic('DO', 'C');



-----------------------3.10
CREATE OR REPLACE FUNCTION PROCENT_SUCCESS_BIRTHDAY() 
RETURNS TABLE (SuccessfulChecks FLOAT, UnSuccessfulChecks FLOAT) 
AS $$ 
	WITH birthday_peers AS (
    SELECT nickname,
      EXTRACT(MONTH FROM birthday) AS mounth_peers,
      EXTRACT(DAY FROM birthday) AS day_peers
    FROM peers
  ), check_date_success AS (
    SELECT peer,
      EXTRACT(MONTH FROM check_date) AS mounth_checks_success,
      EXTRACT(DAY FROM check_date) AS DAY_checks_success
    FROM checks
      JOIN p2p
        ON p2p.check_id = checks.id
    WHERE p2p.state = 'success'
  ), check_date_failure AS (
    SELECT peer,
      EXTRACT(MONTH FROM check_date) AS mounth_checks_failure,
      EXTRACT(DAY FROM check_date) AS DAY_checks_failure
    FROM checks
      JOIN p2p
        ON p2p.check_id = checks.id
    WHERE p2p.state = 'failure'
  ), success_count AS (
    SELECT count(*) AS success_count
    FROM birthday_peers
      JOIN check_date_success
        ON check_date_success.peer = birthday_peers.nickname
    WHERE mounth_peers = mounth_checks_success
      AND day_peers = day_checks_success
    ), failure_count AS (
        SELECT count(*) AS failure_count
        FROM birthday_peers
          JOIN check_date_failure
            ON check_date_failure.peer = birthday_peers.nickname
        WHERE mounth_peers = mounth_checks_failure
          AND day_peers = day_checks_failure
    )
  SELECT CAST(
      round(success_count / sum(success_count + failure_count) * 100, 2) AS FLOAT
    ) AS SuccessfulChecks,
    CAST(
      round(
        failure_count / sum(success_count + failure_count) * 100, 2) AS FLOAT
    ) AS UnsuccessfulChecks
  FROM success_count, failure_count
  GROUP BY success_count, failure_count;
$$ LANGUAGE SQL;
SELECT *
FROM procent_success_birthday();



-----------------------3.11
CREATE OR REPLACE FUNCTION CHECK_PERSON(EX1 VARCHAR, EX2 VARCHAR, EX3 VARCHAR)
RETURNS TABLE(PEER VARCHAR)
AS $$
  SELECT checks.peer
  FROM checks
    JOIN p2p
      ON p2p.check_id = checks.id
    LEFT JOIN verter
      ON verter.check_id = checks.id
  WHERE checks.task = EX1 
    AND p2p.state = 'success'
    AND (verter.state = 'success' OR verter.state ISNULL)
  INTERSECT
  SELECT checks.peer
  FROM checks
    JOIN p2p
      ON p2p.check_id = checks.id
    LEFT JOIN verter
      ON verter.check_id = checks.id
  WHERE checks.task = EX2
    AND p2p.state = 'success'
    AND (verter.state = 'success' OR verter.state ISNULL)
  INTERSECT
  SELECT checks.peer
  FROM checks
    JOIN p2p
      ON p2p.check_id = checks.id
    LEFT JOIN verter
      ON verter.check_id = checks.id
  WHERE checks.task = EX3
    AND (p2p.state = 'failure' AND verter.state ISNULL)
    OR (p2p.state = 'success' AND verter.state = 'failure')
  GROUP BY 1;
$$ LANGUAGE SQL;
SELECT *
FROM check_person(
  'C2_SimpleBashUtils',
  'C3_s21_string+',
  'C5_s21_decimal'
);



-----------------------3.12
CREATE OR REPLACE FUNCTION RecursiveTasks()
RETURNS TABLE(Task VARCHAR, prev_count NUMERIC) AS $$
  WITH RECURSIVE tasks_count(t_title, parent, step) AS (
    SELECT title, parent_task, 0
    FROM tasks
    UNION
    SELECT t_title, tasks.parent_task, 1
    FROM tasks_count AS pv
      JOIN tasks
        ON pv.parent = tasks.title
    WHERE pv.parent IS NOT NULL
  )
  SELECT t_title, SUM(step)
  FROM tasks_count
  GROUP BY 1
  ORDER BY 2 DESC;
$$ LANGUAGE SQL;
SELECT *
FROM RecursiveTasks();



-----------------------3.13
CREATE OR REPLACE FUNCTION LuckyDays(N NUMERIC)
RETURNS TABLE(lucky_day DATE) 
AS $$
  WITH pv AS (
    SELECT *
    FROM checks
      JOIN p2p
        ON p2p.check_id = checks.id
      LEFT JOIN verter
        ON verter.check_id = checks.id
      JOIN tasks
        ON tasks.title = checks.task
      JOIN xp
        ON xp.check_id = checks.id
    WHERE p2p.state = 'success'
      AND (verter.state = 'success' OR verter.state ISNULL)
  )
  SELECT check_date
  FROM pv
  WHERE pv.xp_amount >= pv.max_xp * 0.8
  GROUP BY check_date
  HAVING COUNT(check_date) >= N;
$$ LANGUAGE SQL;
SELECT *
FROM LuckyDays(2);



-----------------------3.14
CREATE OR REPLACE FUNCTION MaxXp()
RETURNS TABLE(peer VARCHAR, xp NUMERIC) AS $$
  SELECT checks.peer, SUM(xp_amount) as _xp_
  FROM xp
    JOIN checks
      ON checks.id = xp.check_id
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 1
$$ LANGUAGE SQL;
SELECT *
FROM MaxXp();



-----------------------3.15
CREATE OR REPLACE FUNCTION CameBefore(b_time TIME, N NUMERIC)
RETURNS TABLE(peer VARCHAR) AS $$
  WITH pv AS (
    SELECT peer, count(peer) as cnt 
    FROM time_tracking tt
    WHERE tt.time < b_time 
      AND state = 1
    GROUP BY 1
  )
  SELECT peer
  FROM pv
  WHERE cnt >= N;
$$ LANGUAGE SQL;
SELECT *
FROM CameBefore('12:00', 2);



-----------------------3.16
CREATE OR REPLACE FUNCTION LastExits(N INTEGER, M NUMERIC)
RETURNS TABLE(peer VARCHAR) AS $$
  WITH pv AS (
    SELECT peer, count(peer) as cnt 
    FROM time_tracking tt
    WHERE state = 2
      AND tt.DATE >= CURRENT_DATE - N
      AND tt.DATE != CURRENT_DATE
    GROUP BY 1
  )
  SELECT peer
  FROM pv
  WHERE cnt > M
$$ LANGUAGE SQL;
SELECT *
FROM LastExits(70, 2);



-----------------------3.17
CREATE OR REPLACE FUNCTION PROCENT_EARLY_ENTERS() 
RETURNS TABLE(MONTH VARCHAR, EARLYENTRIES FLOAT) 
AS $$ 
	WITH all_enter AS (
	  SELECT nickname,
	    EXTRACT(MONTH FROM birthday) AS mounth_birthday,
	    EXTRACT(MONTH FROM DATE) AS mounth_tracking,
	    TIME
	  FROM peers
	    JOIN TIME_tracking
        ON peers.nickname = TIME_tracking.peer
	), enter_count AS (
    SELECT mounth_birthday AS all_mounth_birthday,
	    COUNT(mounth_birthday) AS all_count
    FROM all_enter
	  GROUP BY mounth_birthday, mounth_tracking
	  HAVING mounth_birthday = mounth_tracking
	), early_enter_count AS (
	  SELECT mounth_birthday AS early_mounth_birthday,
	    COUNT(mounth_birthday) AS early_count
	  FROM all_enter
	  GROUP BY mounth_birthday, mounth_tracking, TIME
    HAVING mounth_birthday = mounth_tracking
      AND TIME < '12:00:00'
	)
	SELECT to_char(make_DATE(2000, CAST(all_mounth_birthday AS INTEGER), 1), 'Month'),
	  round(
      CAST(CAST(early_count AS FLOAT) / CAST(all_count AS FLOAT) * 100 AS NUMERIC), 2
    )
	FROM enter_count
	  JOIN early_enter_count
      ON enter_count.all_mounth_birthday = early_enter_count.early_mounth_birthday;
	$$ LANGUAGE SQL;
SELECT *
FROM PROCENT_EARLY_ENTERS();
