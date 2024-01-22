-- Подготовка

SET search_path TO f1_lab3;
SET ROLE postgres;

REVOKE ALL PRIVILEGES ON SCHEMA f1 FROM test;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA f1 FROM test;
REVOKE ALL ON DATABASE f1_lab3 FROM test;
DROP ROLE IF EXISTS test;


REVOKE ALL PRIVILEGES ON SCHEMA f1 FROM statistics_role;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA f1 FROM statistics_role;
DROP ROLE IF EXISTS statistics_role;

---------------------------------------------------------------------------------------------------------------------------------------

-- Создание нового пользователя
CREATE USER test WITH PASSWORD '12345';
-- Разрешение на подключение к базе данных
GRANT CONNECT ON DATABASE f1_lab3 TO test;
-- Предоставление прав "USAGE"
GRANT USAGE ON SCHEMA f1 TO test;


-- Предоставление прав SELECT, INSERT, UPDATE
GRANT SELECT, UPDATE, INSERT ON f1.result to test;
GRANT SELECT(id), UPDATE(first_name, last_name) ON f1.driver to test;
GRANT SELECT(id) ON f1.team to test;

---------------------------------------------------------------------------------------------------------------------------------------

-- Создание представлений
-- Объединение всей информации о гонках и их результатах
CREATE OR REPLACE VIEW result_detailed_info AS
SELECT
    ra.id AS race_id,
    ra.status AS race_status,
    ra.date,
    t.city,
    t.len,
    ra.laps,
    te.id AS team_id,
    te.team_name,
    d.id AS driver_id,
    d.first_name,
    d.last_name,
    r.status AS driver_status,
    r.points
FROM result r
JOIN race ra ON r.race_id = ra.id
JOIN track t ON ra.track_id = t.id
JOIN driver d ON r.driver_id = d.id
JOIN team te ON r.team_id = te.id;


-- Сумма баллов для каждого пилота в сезоне
CREATE OR REPLACE VIEW driver_statistics AS
SELECT
    d.id AS driver_id,
    d.first_name,
    d.last_name,
    (YEAR FROM raEXTRACT.date) AS season,
    SUM(r.points) AS total_points
FROM driver d
JOIN result r ON d.id = r.driver_id
JOIN race ra ON r.race_id = ra.id
GROUP BY d.id, season;


-- Сумма баллов для каждой команды в сезоне
CREATE OR REPLACE VIEW team_statistics AS
SELECT
    t.id AS team_id,
    t.team_name,
    EXTRACT(YEAR FROM ra.date) AS season,
    SUM(r.points) AS total_points
FROM team t
JOIN result r ON t.id = r.team_id
JOIN race ra ON r.race_id = ra.id
GROUP BY t.id, season
ORDER BY t.id, season;


-- Ограниченная инфорация о гонке (!!!)
CREATE OR REPLACE VIEW race_info AS
SELECT
    race.id,
    race.date,
    race.status
FROM race
ORDER BY race.id;


-- Предстоящие гонки (дата и место) (!!!)
CREATE OR REPLACE VIEW upcoming_races AS
SELECT
    race.id AS race_id,
    race.date,
    track.city
FROM race
JOIN track ON race.track_id = track.id
WHERE race.status = 'upcoming';


-- присвоить пользователю test право доступа (SELECT)
GRANT SELECT ON upcoming_races TO test; 
---------------------------------------------------------------------------------------------------------------------------------------


-- Назначить новому пользователю созданную роль
CREATE ROLE statistics_role;
GRANT SELECT(id), UPDATE(date, status) ON race_info TO statistics_role;
GRANT statistics_role TO test;

---------------------------------------------------------------------------------------------------------------------------------------

-- Примеры SELECT запросов
SET ROLE postgres;
SET ROLE test;

 

-- Нет прав на customer, ordering, ticket, track, race, driver_statistics, team_statistics, result_detailed_info
SELECT * FROM customer;
SELECT * FROM ordering;
SELECT * FROM ticket;
SELECT * FROM track;
SELECT * FROM race;
SELECT * FROM driver_statistics; 
SELECT * FROM team_statistics; 
SELECT * FROM result_detailed_info;


-- Ограниченный доступ (не ко всем стобцам) race_info, team, driver
SELECT * FROM race_info;
SELECT id FROM race_info;
SELECT * FROM team;
SELECT id FROM team;
SELECT * FROM driver;
SELECT id FROM driver;



-- Полный доступ
SELECT * FROM result;
SELECT * FROM upcoming_races;


---------------------------------------------------------------------------------------------------------------------------------------

-- Примеры UPDATE/INSERT запросов
-- Права UPDATE есть только к: result, driver, race_info
-- Права INSERT есть только к: result


-- Отказано в доступе INSERT (в driver только select и update)
UPDATE driver SET first_name = 'Maxim' WHERE id = 1;
INSERT INTO driver (first_name, last_name) VALUES ('Maxim', 'Kruglikov');


-- Просто изменять VIEW можно только когда они связаны только с 1 таблицей (иначе нужны триггеры)
-- При изменении VIEW и TABLE синхронизированы
UPDATE race_info SET status = 'passed' WHERE id = 1;
UPDATE race SET status = 'cancelled' WHERE id = 1;

SELECT * FROM race WHERE id = 1;
SELECT * FROM race_info WHERE id = 1;


-- result
UPDATE result SET points = 0 WHERE driver_id = 57 AND race_id = 4;
UPDATE result SET points = 12 WHERE driver_id = 57 AND race_id = 4;
INSERT INTO result (team_id, driver_id, race_id, status) VALUES (1, 1, 1, 'missed');
DELETE FROM result WHERE driver_id = 1 AND race_id = 1;