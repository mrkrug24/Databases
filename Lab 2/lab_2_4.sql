SET search_path TO formula_1;

-- ТРАНЗАКЦИИ

-- 1 - READ UNCOMMITTE 
-- грязные чтения           предотвращает (в PostgreSQL) - см пример 2
-- потерянные изменения     предотвращает                   

BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    UPDATE race SET race_status = 'canceled' WHERE  date = '2023-09-24'; -- (1)
	SELECT * FROM race; -- (3)
COMMIT;

BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    UPDATE race SET race_status = 'passed' WHERE  date = '2023-09-24'; -- (2) бужет ждать commit 
COMMIT; -- (4)

-------------------------------------------------------------------------------


-- 2 - READ COMMITTED
-- неповторяющееся чтение   не предотвращает
-- грязные чтения           предотвращает
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
    UPDATE race SET race_status = 'canceled' WHERE  date = '2023-09-24'; 
COMMIT;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SELECT * FROM race; -- моежт не показать изменения (нужен commit)
    SELECT * FROM race; -- может показать изменения (если update + commit)
COMMIT;

-------------------------------------------------------------------------------


-- 3 - REPEATABLE READ
-- фантомное чтение         предотвращает (в PostgreSQL)
-- неповторяющееся чтение   предотвращает
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    SELECT * FROM race WHERE laps = 57; -- не учитывает изменения других транзакций
    SELECT * FROM race WHERE laps = 57; -- те же значения, тк видны только зафиксированные данные до начала транзакции
COMMIT;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    INSERT INTO race (id, race_status, date, laps) VALUES (24, 'upcoming', TO_DATE('15.12.2023', 'DD.MM.YYYY'), 57);
COMMIT;

-------------------------------------------------------------------------------


-- 4 - SERIALIZABLE
-- фантомное чтение     предотвращает       (как и REPEATABLE READ)
-- ОШИБКА:              не удалось сериализовать доступ из-за зависимостей чтения/записи между транзакциями 

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    UPDATE race SET race_status = 'canceled' WHERE id = 20; --(1)
COMMIT;


BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    UPDATE race SET race_status = 'passed' WHERE id = 20; -- (2) ждет + ERROR:  could not serialize access due to concurrent update + ROLLBACK
COMMIT;


BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT x := start_date FROM exhibitions WHERE id = 3;
    UPDATE exhibitions SET start_date = x + INTERVAL '3 days' WHERE id = 3;
COMMIT;


BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; 
    SET @x := (SELECT start_date FROM exhibitions WHERE id = 3); 
    UPDATE exhibitions SET start_date = @x + INTERVAL 3 DAY WHERE id = 3; 
COMMIT; 


BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    UPDATE exhibitions SET start_date = start_date + INTERVAL '5 days' WHERE id = 3;
COMMIT;


BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; 
    DECLARE x DATE; 
    SELECT start_date INTO x FROM exhibitions WHERE id = 3;
    UPDATE exhibitions SET start_date = x + INTERVAL '3 days' WHERE id = 3; 
COMMIT; 


BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; 
    WITH x AS (SELECT start_date FROM exhibitions WHERE id = 3 ) 
    UPDATE exhibitions SET start_date = (SELECT start_date FROM x) + INTERVAL '3 days' WHERE id = 3; 
COMMIT; 




-- ТРИГГЕРЫ

-- 1. Заполняет поле race_status (passed или upcomming) исходя из запланированных и текущих дат
CREATE OR REPLACE FUNCTION race_date_check()
RETURNS TRIGGER AS $$
BEGIN
    -- если страрый статус - canceled
    IF OLD.race_status = 'canceled' THEN
        RETURN NULL;
    END IF;

    -- если новый статус - canceled
    IF NEW.race_status = 'canceled' THEN
        RETURN NEW;
    END IF;

    -- в остальных случаях 'passed' или 'upcoming'
    IF NEW.date < CURRENT_DATE THEN
        NEW.race_status := 'passed';
    ELSIF NEW.date >= CURRENT_DATE THEN
        NEW.race_status := 'upcoming';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER race_date_trigger
BEFORE INSERT OR UPDATE ON race
FOR EACH ROW
EXECUTE FUNCTION race_date_check();

-- Проверка
INSERT INTO race (id, date, laps) VALUES (26, '2023-09-15', 57);
SELECT * FROM race WHERE id = 26;

-------------------------------------------------------------------------------


-- 2. Заполняет таблицу result при вставке нового пилота
CREATE OR REPLACE FUNCTION fill_result_for_new_driver()
RETURNS TRIGGER AS $$
BEGIN
     -- Вставляем записи для пропущеных гонок
    INSERT INTO result (driver_id, race_id, driver_status)
    SELECT NEW.id, race.id, 'missed'
    FROM race
    WHERE race.race_status IN ('passed', 'canceled');

    -- Вставляем записи для предстоящих гонок
    INSERT INTO result (driver_id, race_id, driver_status)
    SELECT NEW.id, race.id, 'upcoming'
    FROM race
    WHERE race.race_status = 'upcoming';

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER new_driver_insert
AFTER INSERT ON driver
FOR EACH ROW
EXECUTE FUNCTION fill_result_for_new_driver();

-- Проверка
INSERT INTO driver VALUES (6, 1, 'Max', 'Kruglikov', '2003-04-06', 'Russia');

SELECT * FROM result WHERE driver_id = 6;

-------------------------------------------------------------------------------


-- 3. При смене race_status: 'upcoming' -> 'canceled' меняет driver_status: 'upcoming' -> 'missed'
CREATE OR REPLACE FUNCTION driver_status_check()
RETURNS TRIGGER AS $$
BEGIN
    -- вносим изменения
    IF OLD.race_status = 'upcoming' AND NEW.race_status = 'canceled' THEN
        UPDATE result SET driver_status = 'missed' WHERE race_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER driver_status_trigger
BEFORE INSERT OR UPDATE ON race
FOR EACH ROW
EXECUTE FUNCTION driver_status_check();

-- Проверка
SELECT * FROM race WHERE id = 19;
UPDATE race SET race_status = 'canceled' WHERE id = 19;

SELECT * FROM race WHERE id = 19;

SELECT * FROM result WHERE race_id =19;

-------------------------------------------------------------------------------


-- 4. Контроль связи между driver_status и race_status
CREATE OR REPLACE FUNCTION race_driver_status_check()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM race r
        INNER JOIN result rs ON r.id = rs.race_id
        WHERE (
            (r.race_status = 'upcoming' AND rs.driver_status != 'upcoming') OR
            (r.race_status != 'upcoming' AND rs.driver_status = 'upcoming') OR
            (r.race_status = 'canceled' AND rs.driver_status != 'missed') OR
            (r.race_status = 'passed' AND rs.driver_status = 'upcoming')
        ))
    THEN
        RAISE EXCEPTION 'Invalid driver and race status';
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER race_driver_status_trigger
AFTER INSERT OR UPDATE ON result
FOR EACH ROW
EXECUTE FUNCTION race_driver_status_check();


CREATE OR REPLACE TRIGGER driver_race_status_trigger
AFTER INSERT OR UPDATE ON race
FOR EACH ROW
EXECUTE FUNCTION race_driver_status_check();


UPDATE race SET race_status = 'canceled' WHERE id = 10;