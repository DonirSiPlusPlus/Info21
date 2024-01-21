--Создание и заполнение базы
CREATE DATABASE part4;

CREATE TABLE table_a(value) AS
  SELECT *
  FROM (VALUES('a'),('a'),('a')) table_a(value);

CREATE TABLE table_b(value) AS
  SELECT *
  FROM (VALUES('b'),('c'),('c'),('d'),('e')) table_B(value);

CREATE TABLE c_table_c(value) AS
  SELECT *
  FROM (VALUES('c'),('c'),('d'),('e')) c_table_c(value);

CREATE TABLE asaf(value) AS
  SELECT *
  FROM (VALUES('a'),('a'),('a')) asaf(value);

CREATE TABLE asaft(value) AS
  SELECT *
  FROM (VALUES('a'),('a'),('a')) asaft(value);

CREATE OR REPLACE FUNCTION fnc_trg_a() RETURNS TRIGGER
AS $$
BEGIN
  RAISE NOTICE 'fnc_trg_a';
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_trg_b() RETURNS TRIGGER
AS $$
BEGIN
  RAISE NOTICE 'fnc_trg_b';
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_a
  AFTER INSERT ON table_a
  FOR EACH ROW
  EXECUTE FUNCTION fnc_trg_a();

CREATE OR REPLACE TRIGGER trg_b
  AFTER INSERT ON table_b
  FOR EACH ROW
  EXECUTE FUNCTION fnc_trg_b();

CREATE OR REPLACE FUNCTION func_calc_elements_in_table_a (OUT count_of_outs numeric)
RETURNS numeric
AS $$
  SELECT count(*) AS count_of_outs
  FROM table_a
  GROUP BY value
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION func_calc_elements_in_table_b (OUT count_of_outs numeric)
RETURNS numeric
AS $$
  SELECT count(*) AS count_of_outs
  FROM table_b
  GROUP BY value     
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION func_show_calc_elements_in_table_a_without_params()
RETURNS numeric
AS $$
  SELECT count(*) AS count_of_outs
  FROM table_a
  GROUP BY value     
$$ LANGUAGE SQL;


--4.1
CREATE OR REPLACE PROCEDURE drop_tables_by_pattern(IN pattern VARCHAR)
AS $$
DECLARE
  find_pattern VARCHAR;
BEGIN
  LOOP
      find_pattern := (SELECT table_name
                         FROM information_schema.tables
                        WHERE table_schema ='public'
                          AND table_name LIKE CONCAT(pattern,'%')
                        LIMIT 1);
      IF find_pattern IS NOT NULL
      THEN
          RAISE NOTICE 'Delete % ', find_pattern;
          EXECUTE format('DROP TABLE IF EXISTS %I CASCADE',find_pattern);
      ELSE
          EXIT;
      END IF;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

----Вызов
CALL drop_tables_by_pattern('tab');
CALL drop_tables_by_pattern('asa');
CALL drop_tables_by_pattern('c');

--4.2
CREATE OR REPLACE PROCEDURE show_scalar_funcs_with_params (INOUT count_of_funcs integer)
AS $$
DECLARE
  lists_string RECORD;
  output_string VARCHAR;
BEGIN
  count_of_funcs=0;
  output_string = '';
  RAISE NOTICE 'List of funcs with params: ';
  FOR lists_string IN (
    SELECT r.routine_name AS func_name, p.data_type AS param_type
    FROM information_schema.routines AS r
      JOIN information_schema.parameters AS p
        ON r.specific_name = p.specific_name
    WHERE r.specific_schema = 'public'
      AND r.routine_type='FUNCTION'
      AND r.data_type != 'trigger'
    ORDER BY func_name
  )
  LOOP
    count_of_funcs = count_of_funcs + 1;
    output_string = output_string || lists_string.func_name || '(' || lists_string.param_type || ')'|| ', ';
  END LOOP;
  RAISE NOTICE '%', output_string;
END;
$$ LANGUAGE PLPGSQL;

----Вызов
DO $$ DECLARE func_count INTEGER;
  BEGIN CALL show_scalar_funcs_with_params(func_count);
  RAISE NOTICE 'Количество найденных функций: %', func_count;
END $$;


--4.3
CREATE OR REPLACE PROCEDURE drop_triggers(INOUT count_of_triggers INTEGER)
AS $$
DECLARE
  trigger_name_pattern VARCHAR;
  table_name_pattern VARCHAR;
BEGIN
  count_of_triggers = 0;
  LOOP
      trigger_name_pattern := (SELECT trigger_name
                                 FROM information_schema.triggers
                                WHERE trigger_catalog ='part4'
                                  AND event_manipulation
                                   IN ('INSERT', 'UPDATE', 'MERGE','DELETE')
                                LIMIT 1);
      IF trigger_name_pattern IS NOT NULL
      THEN
          table_name_pattern := (SELECT event_object_table
                                   FROM information_schema.triggers
                                  WHERE trigger_name = trigger_name_pattern);
          count_of_triggers = count_of_triggers + 1;
          RAISE NOTICE 'Delete trigger % from table %', trigger_name_pattern, table_name_pattern;
          EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I CASCADE',trigger_name_pattern, table_name_pattern);
      ELSE
          EXIT;
      END IF;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

----Вызов
DO $$ DECLARE trigger_count INTEGER;
  BEGIN CALL drop_triggers(trigger_count);
  RAISE NOTICE 'Количество найденных триггеров: %', trigger_count;
END $$;

--4.4
CREATE OR REPLACE PROCEDURE find_func_and_procedures_by_pattern (IN pattern VARCHAR)
AS $$
DECLARE 
  lists_string RECORD;
BEGIN
  RAISE NOTICE 'List of funcs with #%# in defenition: ', pattern;
  FOR lists_string IN (
    SELECT *
    FROM information_schema.routines AS r
    WHERE r.specific_schema = 'public'
      AND ( data_type != 'trigger' OR data_type IS NULL )
      AND r.routine_definition LIKE CONCAT('%', pattern, '%')
    ORDER BY r.routine_name
  )
  LOOP
    RAISE NOTICE 'Name: % Type: %', lists_string.routine_name, lists_string.routine_type;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

----Вызов
CALL find_func_and_procedures_by_pattern('count');
CALL find_func_and_procedures_by_pattern('RAISE');
