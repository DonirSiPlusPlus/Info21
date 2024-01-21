
--------------1

CREATE OR REPLACE PROCEDURE ADDP2P(CHECKING_NICKNAME VARCHAR, NICKNAME VARCHAR, NAME_EXERCISE VARCHAR, STATUS_CHECK_COL STATUS_CHECK, TIME_P2P TIME)
AS $$
DECLARE
	p2p_id BIGINT;
	check_id BIGINT;
	check_id_ns VARCHAR;
BEGIN
	p2p_id = (SELECT max(ID) + 1 FROM P2P);
	IF status_check_col = 'start' THEN check_id = (SELECT max(ID) + 1
  																								 FROM checks);
		INSERT INTO checks
		VALUES (
		        check_id,
		        NICKNAME,
		        NAME_EXERCISE,
		        current_date
			    );
		INSERT INTO p2p
		VAlUES (
		        p2p_id,
		        check_id,
		        CHECKING_NICKNAME,
		        'start',
		        TIME_p2p
		  	  );
	ELSE
		INSERT INTO p2p
		VALUES(
		        p2p_id, (SELECT checks.id
		                   FROM p2p
		                   JOIN checks ON p2p.check_id = checks.id
		                  WHERE state = 'start'
		                    AND task = NAME_EXERCISE
		                    AND peer = NICKNAME
		                    AND checking_peer = CHECKING_NICKNAME
		                  LIMIT 1),
						CHECKING_NICKNAME, STATUS_CHECK_COL, TIME_p2p
		    );
	END IF;
END;
$$ LANGUAGE PLPGSQL; 

CALL addp2p (
        'bizarrol',
        'birdpers',
        'C2_SimpleBashUtils',
        'start',
        localtime(0)
    );

CALL addp2p (
        'bizarrol',
        'birdpers',
        'C2_SimpleBashUtils',
        'success',
        localtime(0)
    );

--------------2

CREATE OR REPLACE PROCEDURE ADDVERTER(NICKNAME VARCHAR, NAME_EXERCISE VARCHAR, SATUS_VERTER STATUS_CHECK, TIME_CHECK TIME)
AS $$ 
INSERT INTO verter(id, check_id, state, time)
VALUES( (SELECT max(id) + 1
	         FROM verter),
				(SELECT check_id
					 FROM p2p
					 JOIN checks ON p2p.check_id = checks.id
	        WHERE state = 'success'
	          AND checking_peer = NICKNAME
	          AND task = NAME_EXERCISE
	     ORDER BY check_time DESC
	        LIMIT 1),
				SATUS_VERTER, TIME_CHECK);
$$ LANGUAGE SQL; 

CALL addverter (
        'bizarrol',
        'C2_SimpleBashUtils',
        'start',
        localtime(0)
    );

CALL addverter (
        'bizarrol',
        'C2_SimpleBashUtils',
        'success',
        localtime(0)
    );

--------------3

CREATE OR REPLACE FUNCTION INSERT_TRANSFER_POINT() 
RETURNS TRIGGER
AS $$ 
BEGIN
	INSERT INTO transferred_points (id, checking_peer, checked_peer, points_amount)
	VALUES ((SELECT max(id) + 1
	           FROM transferred_points),
					(SELECT checking_peer
	           FROM p2p
	           JOIN checks ON p2p.check_id = checks.id
	          WHERE p2p.id = (SELECT max(id)
					                    FROM p2p)),
					(SELECT peer
	           FROM p2p
	           JOIN checks ON p2p.check_id = checks.id
	          WHERE p2p.id = (SELECT max(id)
					                    FROM p2p)),
	      	1);
	RETURN NULL;
END;
$$ LANGUAGE PLPGSQL; 

CREATE OR REPLACE TRIGGER TRANSFER_POINTS_TRIGGER 
AFTER
INSERT ON P2P FOR EACH ROW
  WHEN(NEW.STATE = 'start')
EXECUTE PROCEDURE INSERT_TRANSFER_POINT(); 

--------------4

CREATE OR REPLACE TRIGGER CHECK_XP 
BEFORE
INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION INSERT_XP(); 

CREATE OR REPLACE FUNCTION INSERT_XP()
RETURNS TRIGGER 
AS $$
DECLARE
	NoXP BOOLEAN;
	CorrXP INTEGER;
	SuccVerter BOOLEAN;
	NoVerter BOOLEAN;
	SuccP2P BOOLEAN;
	SuccXP BOOLEAN;
BEGIN
	NoXP = (SELECT id
	         FROM xp
	        WHERE xp.check_id = NEW.check_id
	        LIMIT 1) IS NULL;
	CorrXP = (SELECT max_xp
	            FROM tasks
	        		JOIN checks ON checks.task = tasks.title
	    			 WHERE checks.id = NEW.check_id);
	SuccVerter = (SELECT State
									FROM verter
	    					 WHERE verter.check_id = NEW.check_id
	        				 AND verter.state = 'success'
	    					 LIMIT 1) IS NOT NULL;
	NoVerter = (SELECT state
								FROM verter
	    					WHERE verter.check_id = NEW.check_id
								LIMIT 1) IS NULL;
	SuccP2P = (SELECT state
							 FROM p2p
	    				WHERE p2p.check_id = NEW.check_id
	    			    AND p2p.State = 'success'
	    				LIMIT 1) IS NOT NULL;
	SuccXP = NoXP AND (SuccVerter OR (NoVerter AND SuccP2P));
	IF
		NEW.xp_amount > CorrXP OR NOT SuccXP THEN NEW = NULL;
	END IF;
	RETURN NEW;
	END;
	$$ LANGUAGE 
PLPGSQL; 

INSERT INTO xp (id, check_id, xp_amount)
VALUES ( (SELECT max(id) + 1
            FROM xp),
        10,
        100);