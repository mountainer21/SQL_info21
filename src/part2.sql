-- DROP PROCEDURE IF EXISTS add_p2p_check() CASCADE;
-- DROP PROCEDURE IF EXISTS add_verter_check() CASCADE;
-- DROP FUNCTION IF EXISTS fnc_trg_p2p_start_insert() CASCADE;
-- DROP FUNCTION IF EXISTS fnc_insert_check_xp() CASCADE;

--- 1) Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer varchar,
    checker_peer varchar,
    task_name varchar,
    p2p_status CheckStatus,
    check_time time
)
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF p2p_status = 'Start'
    THEN
        IF (SELECT "State"
            FROM p2p
                     INNER JOIN checks
                                ON task = task_name
            WHERE checkingpeer = checker_peer
            ORDER BY "Time" DESC
            limit 1) = 'Start'
        THEN
            RAISE EXCEPTION 'Start record already exists';
        ELSE
            INSERT INTO checks(peer, task, "Date")
            VALUES (checked_peer, task_name, now());
        END IF;
    ELSEIF (SELECT "State"
            FROM p2p
                     INNER JOIN checks
                                ON task = task_name
            WHERE checkingpeer = checker_peer
            ORDER BY "Time" DESC
            limit 1) <> 'Start'
    THEN
        RAISE EXCEPTION 'No start record';
    END IF;
    INSERT INTO p2p("Check", checkingpeer, "State", "Time")
    SELECT checks.id, checker_peer, p2p_status, now()
    FROM checks
    WHERE peer = checked_peer
      AND task = task_name
    ORDER BY "Date" DESC
    limit 1;
END
$$;

-- 2) Написать процедуру добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE add_verter_check(checking_peer varchar,
                                    checking_task varchar,
                                    checking_status CheckStatus,
                                    checking_time time)
    LANGUAGE plpgsql
AS
$$
DECLARE
    check_ bigint = (SELECT c.id
                     FROM checks c
                              JOIN p2p p ON c.id = p.id
                     WHERE c.task = checking_task
                       AND c.peer = checking_peer
                       AND p."State" = 'Success'
                     ORDER BY p."Time" DESC
                     LIMIT 1);
BEGIN
    IF check_ IS NOT NULL
    THEN
        IF (NOT EXISTS(SELECT id FROM verter WHERE id = check_ AND "State" = checking_status))
        THEN
            INSERT INTO Verter (id, "State", "Time")
            VALUES (check_, checking_status, checking_time);
        END IF;
    END IF;
END;
$$;

-- 3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_trg_p2p_start_insert()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW."State" = 'Start' THEN
        IF EXISTS(SELECT *
                  FROM transferredpoints
                  WHERE checkingpeer = NEW.checkingpeer
                    AND checkedpeer  =
                        (SELECT peer
                         FROM checks
                                  INNER JOIN p2p ON p2p."Check" = checks.id
                         WHERE p2p.id = NEW.id))
        THEN
            UPDATE transferredpoints
            SET pointsamount = pointsamount + 1
            WHERE checkingpeer = NEW.checkingpeer
              AND checkedpeer  =
                  (SELECT peer
                   FROM checks
                            INNER JOIN p2p ON p2p."Check" = checks.id
                   WHERE p2p.id = NEW.id);
        ELSE
            INSERT INTO transferredpoints(checkingpeer, checkedpeer , pointsamount)
            SELECT NEW.checkingpeer, checks.peer, 1
            FROM checks
                     INNER JOIN p2p ON p2p."Check" = checks.id
            WHERE p2p.id = NEW.id;
        END IF;
    END IF;
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_insert
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_p2p_start_insert();

-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
CREATE
    OR REPLACE FUNCTION fnc_insert_check_xp() RETURNS TRIGGER AS
$$
BEGIN
    IF (SELECT "State"
        FROM p2p p
        WHERE p."Check" = new."Check"
          AND p."State" IN ('Success', 'Failure')) = 'Failure' THEN
        RAISE EXCEPTION 'Запись не прошла проверку p2p';
    ELSEIF (SELECT "State"
            FROM verter v
            WHERE v."Check" = new."Check"
              AND v."State" IN ('Success', 'Failure')) = 'Failure' THEN
        RAISE EXCEPTION 'Запись не прошла проверку verter';
    ELSEIF (SELECT maxxp
            FROM checks c
                     JOIN tasks t ON t.title = c.task
            WHERE c.id = new."Check") < new.xpamount THEN
        RAISE EXCEPTION 'Количество XP превышает максимальное доступное для проверяемой задачи';
    ELSE
        RETURN new;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_insert_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_insert_check_xp();

CALL add_p2p_check('greenbea', 'nellyole', 'C3_s21_string+', 'Start', '07:00:00');
CALL add_p2p_check('loewetha', 'nellyole', 'C2_SimpleBashUtils', 'Start', '01:00:00');
CALL add_verter_check('hubertfu', 'C2_SimpleBashUtils', 'Success', '10:01'); --success
CALL add_p2p_check('greenbea', 'nellyole', 'C3_s21_string+', 'Start', '07:00:00');--success
CALL add_p2p_check('loewetha', 'nellyole', 'C2_SimpleBashUtils', 'Start', '01:00:00');--exception

