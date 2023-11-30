-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
-- Ник пира 1, ник пира 2, количество переданных пир поинтов.
-- Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.
--
DROP FUNCTION IF EXISTS get_transferred_points_info() cascade;

CREATE OR REPLACE FUNCTION get_transferred_points_info()
    RETURNS TABLE
            (
                peer1         VARCHAR(255),
                peer2         VARCHAR(255),
                points_amount NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer AS peer1, checkedpeer AS peer2, sum(PointsChange) AS points_amount
        FROM ((SELECT checkingpeer AS peer, checkedpeer, sum(pointsamount) AS PointsChange
               FROM transferredpoints
               GROUP BY checkingpeer, checkedpeer)
              UNION
              (SELECT checkedpeer AS peer, checkingpeer, -sum(pointsamount) AS PointsChange
               FROM transferredpoints
               GROUP BY checkedpeer, checkingpeer))
        GROUP BY peer, checkedpeer;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_transferred_points_info();

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
-- В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks).
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.
--
drop function if exists get_verified_tasks_info() cascade;

CREATE OR REPLACE FUNCTION get_verified_tasks_info()
    RETURNS TABLE
            (
                peer_name TEXT,
                task_name TEXT,
                xp_amount INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer::TEXT AS Peer,
               task::TEXT AS Task,
               xpamount   AS XP
        FROM Checks
                 JOIN xp ON checks.id = xp."Check";
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_verified_tasks_info();

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022.
-- Функция возвращает только список пиров.
--
drop function if exists get_peers_on_campus_whole_day(day DATE) cascade;

CREATE OR REPLACE FUNCTION get_peers_on_campus_whole_day(day DATE)
    RETURNS TABLE
            (
                on_campus_peer_name TEXT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT peer::TEXT
        FROM TimeTracking
        WHERE "Date" = day
        GROUP BY peer
        HAVING COUNT(DISTINCT "State") = 1
           AND MIN("State") = 1;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_peers_on_campus_whole_day('2023-01-01');

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов
--

DROP PROCEDURE IF EXISTS get_points_change cascade;

CREATE OR REPLACE PROCEDURE get_points_change(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer, sum(PointsChange) AS PointsChange
        FROM ((SELECT checkingpeer AS Peer, sum(pointsamount) AS PointsChange
               FROM transferredpoints
               GROUP BY checkingpeer)
              UNION
              (SELECT checkedpeer AS Peer, -sum(pointsamount) AS PointsChange
               FROM transferredpoints
               GROUP BY checkedpeer))
        group by Peer
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_points_change('ref');
FETCH ALL IN "ref";
END;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
--
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов

DROP PROCEDURE IF EXISTS get_points_change_with1task cascade;

CREATE OR REPLACE PROCEDURE get_points_change_with1task(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer1, sum(points_amount) AS PointsChange
        FROM get_transferred_points_info()
        group by peer1
        ORDER BY PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_points_change_with1task('ref');
FETCH ALL IN "ref";
END;

-- 6) Определить самое часто проверяемое задание за каждый день
-- При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все.
-- Формат вывода: день, название задания

DROP PROCEDURE IF EXISTS get_tasks_with_max_count cascade;

CREATE OR REPLACE PROCEDURE get_tasks_with_max_count(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH TaskCounts AS (SELECT "Date",
                                   checks.Task,
                                   COUNT(*)::INTEGER                                        AS Count,
                                   RANK() OVER (PARTITION BY "Date" ORDER BY COUNT(*) DESC) AS r
                            FROM Checks
                            GROUP BY "Date", checks.Task)
        SELECT "Date", TaskCounts.Task
        FROM TaskCounts
        WHERE (TaskCounts.Task, TaskCounts.Count) IN
              (SELECT TaskCounts.Task, MAX(TaskCounts.Count) FROM TaskCounts GROUP BY TaskCounts.Task);
END;
$$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_tasks_with_max_count('ref');
-- FETCH ALL IN "ref";
-- END;

-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP".
-- Результат вывести отсортированным по дате завершения.
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)

DROP PROCEDURE IF EXISTS get_peers_completed_block CASCADE;

CREATE OR REPLACE PROCEDURE get_peers_completed_block(IN block varchar, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH tasks_list AS (SELECT *
                            FROM tasks
                            WHERE title LIKE '%' || block || '%'),
             last AS (SELECT MAX(title) AS title FROM tasks_list),
             finish_date AS (SELECT checks.peer, checks.task, checks."Date"
                             FROM checks
                                      JOIN xp ON checks.id = xp."Check"
                             GROUP BY checks.id)
        SELECT finish_date.peer
                   AS Peer,
               finish_date."Date"
        FROM finish_date
                 JOIN last ON finish_date.task = last.title;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_peers_completed_block('D', 'ref');
FETCH ALL IN "ref";
END;

-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей.
-- Формат вывода: ник пира, ник найденного проверяющего

DROP PROCEDURE IF EXISTS get_recommend_peer CASCADE;

CREATE OR REPLACE PROCEDURE get_recommend_peer(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer, Recommended_Peer
        FROM (SELECT Peer,
                     Recommended_Peer,
                     Recommendations,
                     RANK() OVER (PARTITION BY Peer ORDER BY Recommendations DESC) as rank
              FROM (SELECT Peer, Recommended_Peer, COUNT(*) AS Recommendations
                    FROM (SELECT f.Peer1 AS Peer, r.RecommendedPeer AS Recommended_Peer
                          FROM Friends f
                                   JOIN Recommendations r ON f.Peer2 = r.Peer
                          UNION ALL
                          SELECT f.Peer2 AS Peer, r.RecommendedPeer AS Recommended_Peer
                          FROM Friends f
                                   JOIN Recommendations r ON f.Peer1 = r.Peer) AS subquery
                    GROUP BY Peer, Recommended_Peer) AS subquery2) AS subquery3
        WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;


BEGIN;
CALL get_recommend_peer('ref');
FETCH ALL IN "ref";
COMMIT;


-- 9) Определить процент пиров, которые:
--
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
--
-- Пир считается приступившим к блоку, если он проходил хоть одну проверку любого задания из этого блока (по таблице Checks)
-- Параметры процедуры: название блока 1, например SQL, название блока 2, например A.
-- Формат вывода: процент приступивших только к первому блоку, процент приступивших только ко второму блоку, процент приступивших к обоим, процент не приступивших ни к одному

DROP PROCEDURE IF EXISTS calculate_peer_progress cascade;

CREATE OR REPLACE PROCEDURE calculate_peer_progress(IN block1 varchar, IN block2 varchar, IN ref refcursor) AS
$$
DECLARE
    total_peers BIGINT;
BEGIN
    SELECT COUNT(*) INTO total_peers FROM Peers;
    OPEN ref FOR
        WITH started_1 AS
                 (SELECT DISTINCT peer
                  FROM checks
                  WHERE checks.task LIKE (block1 || '%')),
             started_2 AS
                 (SELECT DISTINCT peer
                  FROM checks
                  WHERE checks.task LIKE (block2 || '%')),
             started_both AS
                 (SELECT *
                  FROM started_1
                  INTERSECT
                  SELECT *
                  FROM started_2),
             started_either AS
                 (SELECT *
                  FROM started_1
                  UNION
                  SELECT *
                  FROM started_2),
             didnt_start AS (SELECT nickname
                             FROM peers
                             EXCEPT
                             SELECT peer
                             FROM started_either)
        SELECT COALESCE(100 * (SELECT COUNT(*)
                               FROM started_1
                               EXCEPT
                               SELECT COUNT(*)
                               FROM started_2) / total_peers, 0)    as StartedBlock1,
               COALESCE(100 * (SELECT COUNT(*)
                               FROM started_2
                               EXCEPT
                               SELECT COUNT(*)
                               FROM started_1) / total_peers, 0)    as StartedBlock2,
               COALESCE(100 * (SELECT COUNT(*)
                               FROM started_both) / total_peers, 0) as StartedBothBlocks,
               COALESCE(100 * (SELECT COUNT(*)
                               FROM didnt_start) / total_peers, 0)  as DidntStartAnyBlock
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL calculate_peer_progress('C', 'D', 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения.
-- Формат вывода: процент пиров, успешно прошедших проверку в день рождения, процент пиров, проваливших проверку в день рождения

DROP PROCEDURE IF EXISTS calculate_birthday_checks CASCADE;

CREATE OR REPLACE PROCEDURE calculate_birthday_checks(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT ROUND(CAST(successful_birthday_checks AS DECIMAL) / total_peers * 100, 2) AS SuccessfulChecks,
               ROUND(CAST(failed_birthday_checks AS DECIMAL) / total_peers * 100, 2)     AS UnsuccessfulChecks
        FROM (SELECT COUNT(DISTINCT p.Nickname)                  AS total_peers,
                     COUNT(DISTINCT CASE
                                        WHEN c."Date" = p.Birthday AND p2p."State" = 'Success'
                                            THEN p.Nickname END) AS successful_birthday_checks,
                     COUNT(DISTINCT CASE
                                        WHEN c."Date" = p.Birthday AND p2p."State" = 'Failure'
                                            THEN p.Nickname END) AS failed_birthday_checks
              FROM Peers p
                       JOIN Checks c ON p.Nickname = c.Peer
                       JOIN P2P p2p ON c.ID = p2p."Check") AS subquery;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL calculate_birthday_checks('ref');
FETCH ALL IN "ref";
COMMIT;

-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3.
-- Формат вывода: список пиров

DROP PROCEDURE IF EXISTS get_without_3d CASCADE;

CREATE OR REPLACE PROCEDURE get_without_3d(IN task1 VARCHAR, IN task2 VARCHAR, IN task3 VARCHAR, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        (SELECT peer
         FROM checks
                  JOIN xp ON xp."Check" = checks.id
         WHERE checks.task = task1)
        INTERSECT
        (SELECT peer
         FROM checks
                  JOIN xp ON xp."Check" = checks.id
         WHERE checks.task = task2)
        EXCEPT
        (SELECT peer
         FROM checks
                  JOIN xp ON xp."Check" = checks.id
         WHERE checks.task = task3);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_without_3d('DO1_Linux', 'C3_s21_string+', 'DO6_CICD', 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей.
-- Формат вывода: название задачи, количество предшествующих
DROP PROCEDURE IF EXISTS get_calculate_prerequisite_tasks cascade;

CREATE OR REPLACE PROCEDURE get_calculate_prerequisite_tasks(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH RECURSIVE prerequisite_tasks AS (SELECT Title, ParentTask, 0 AS Level
                                              FROM Tasks
                                              WHERE ParentTask IS NULL
                                              UNION ALL
                                              SELECT t.Title, t.ParentTask, pt.Level + 1
                                              FROM Tasks t
                                                       INNER JOIN prerequisite_tasks pt ON t.ParentTask = pt.Title)
        SELECT Title AS Task, Level AS PrevCount
        FROM prerequisite_tasks
        ORDER BY PrevCount;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_calculate_prerequisite_tasks('ref');
FETCH ALL IN "ref";
COMMIT;


-- 13) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N.
-- Временем проверки считать время начала P2P этапа.
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных.
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального.
-- Формат вывода: список дней

DROP PROCEDURE IF EXISTS get_lucky_days CASCADE;

CREATE OR REPLACE PROCEDURE get_lucky_days(IN N INTEGER, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT "Date"
        FROM (SELECT "Date",
                     streak_id,
                     COUNT(*) OVER (PARTITION BY "Date", streak_id) AS successful_streak
              FROM (SELECT c."Date",
                           p2p."State",
                           xp.XPAmount,
                           t.MaxXP,
                           CASE WHEN "State" = 'Success' THEN ROW_NUMBER() OVER (ORDER BY c."Date", p2p."Time") END -
                           ROW_NUMBER() OVER (PARTITION BY "State" ORDER BY c."Date", p2p."Time") AS streak_id
                    FROM Checks c
                             JOIN P2P p2p ON c.ID = p2p."Check"
                             JOIN XP xp ON c.ID = xp."Check"
                             JOIN Tasks t ON c.Task = t.Title
                    WHERE p2p."State" IN ('Success', 'Failure')
                      AND xp.XPAmount >= t.MaxXP * 0.8) AS subquery) AS subquery2
        WHERE successful_streak >= N
        GROUP BY "Date";
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_lucky_days(3, 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 14) Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP

DROP PROCEDURE IF EXISTS get_peer_with_max_xp CASCADE;

CREATE OR REPLACE PROCEDURE get_peer_with_max_xp(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer, sum(xpamount) as XP
        FROM xp
                 JOIN checks ON xp."Check" = checks.id
        GROUP BY peer
        ORDER BY XP DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_peer_with_max_xp('ref');
FETCH ALL IN "ref";
COMMIT;

-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N.
-- Формат вывода: список пиров

DROP PROCEDURE IF EXISTS get_visits CASCADE;

CREATE OR REPLACE PROCEDURE get_visits(IN N INTEGER, IN T TIME, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer
        FROM (SELECT peer, min("Time") as min_t
              FROM timetracking
              WHERE "State" = 1
              group by peer, "Date")
        WHERE min_t < T
        GROUP BY peer
        HAVING count(peer) >= N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL get_visits(1, '15:00:00', 'ref');
FETCH ALL IN "ref";
COMMIT;

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
-- Параметры процедуры: количество дней N, количество раз M.
-- Формат вывода: список пиров

DROP PROCEDURE IF EXISTS get_frequent_leavers CASCADE;

CREATE OR REPLACE PROCEDURE get_frequent_leavers(IN N INTEGER, IN M INTEGER, IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer
        FROM (SELECT t.Peer, COUNT(t.Peer) AS ExitCount
              FROM (SELECT Peer,
                           "Date",
                           COUNT("State") FILTER (WHERE "State" = 2) OVER (PARTITION BY Peer, "Date") AS Exits
                    FROM TimeTracking
                    WHERE "Date" > CURRENT_DATE - INTERVAL '1 day' * N) t
              WHERE t.Exits > M
              GROUP BY t.Peer);
END;
$$
    LANGUAGE plpgsql;

-- BEGIN;
-- CALL get_frequent_leavers(0, 0, 'ref');
-- FETCH ALL IN "ref";
-- COMMIT;

-- 17) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус за всё время (будем называть это общим числом входов).
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов).
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов.
-- Формат вывода: месяц, процент ранних входов

DROP PROCEDURE IF EXISTS calculate_early_entries CASCADE;

CREATE OR REPLACE PROCEDURE calculate_early_entries(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT TO_CHAR(TO_DATE(month_number::TEXT, 'MM'), 'Month')                  AS Month,
               COALESCE((early_entries::NUMERIC / total_entries::NUMERIC) * 100, 0) AS EarlyEntries
        FROM (SELECT EXTRACT(MONTH FROM "Date")                                       AS month_number,
                     COUNT(*) FILTER (WHERE "State" = 1)                              AS total_entries,
                     COUNT(*) FILTER (WHERE "State" = 1 AND "Time" < TIME '12:00:00') AS early_entries
              FROM TimeTracking
              GROUP BY month_number);
END;
$$
    LANGUAGE plpgsql;

-- BEGIN;
-- CALL calculate_early_entries('ref');
-- FETCH ALL IN "ref";
-- COMMIT;