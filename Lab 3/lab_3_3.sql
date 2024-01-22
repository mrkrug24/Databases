-- 1. Возвращает таблицу: суммы баллов пилотов по сезонам (ветвление)
CREATE OR REPLACE FUNCTION driver_season_points()
RETURNS TABLE (
    id INT,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    season NUMERIC,
    total_points BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        result.driver_id,
        driver.first_name,
        driver.last_name,
        EXTRACT(YEAR FROM race.date) AS season,
        SUM(CASE WHEN result.status = 'finished' THEN result.points ELSE 0 END) AS total_points
    FROM result
    INNER JOIN race ON result.race_id = race.id
    INNER JOIN driver ON result.driver_id = driver.id
    GROUP BY result.driver_id, season, driver.first_name, driver.last_name
    ORDER BY result.driver_id, season;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM driver_season_points();


-- 2. Возвращает таблицу: суммы баллов команд по сезонам
CREATE OR REPLACE FUNCTION team_season_points()
RETURNS TABLE (
    team_id INT,
    team_name VARCHAR(20),
    season NUMERIC,
    total_points BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        team.id AS team_id,
        team.team_name,
        EXTRACT(YEAR FROM race.date) AS season,
        COALESCE(SUM(result.points), 0) AS total_points
    FROM team
    JOIN result ON team.id = result.team_id
    JOIN race ON result.race_id = race.id
    WHERE result.status = 'finished'
    GROUP BY team.id, team.team_name, season
    ORDER BY team.id, season;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM team_season_points();



-- 3. Возвращает сумму баллов пилота в сезоне по driver_id и season (переменные, обработка ошибок, обращение к аргументам через $n)
CREATE OR REPLACE FUNCTION driver_points(driver_id INT, season INT)
RETURNS INT AS $$
DECLARE
    total_points INT := 0;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM driver) THEN
        RAISE EXCEPTION 'Некорректное значение driver_id';
    END IF;

    IF $2 < 2000 OR $2 > EXTRACT(YEAR FROM NOW()) THEN
        RAISE EXCEPTION 'Некорректное значение season';
    END IF;

    SELECT SUM(result.points) INTO total_points
    FROM result
    JOIN race ON result.race_id = race.id
    WHERE result.driver_id = $1
    AND EXTRACT(YEAR FROM race.date) = $2;

    IF total_points IS NULL THEN
        RAISE EXCEPTION 'Пилот не участвовал в данном сезоне';
    END IF;

    RETURN total_points;
END;
$$ LANGUAGE plpgsql;


SELECT driver_points(1, 2000) AS total_points;
SELECT driver_points(1000, 2000) AS total_points;
SELECT driver_points(1, 2024) AS total_points;
SELECT driver_points(1, 2023) AS total_points;





-- 4. Возвращает сумму баллов команды в сезоне по team_id и season (переменные и исключения)
CREATE OR REPLACE FUNCTION team_points(team_id INT, season NUMERIC)
RETURNS INT AS $$
DECLARE
    total_points INT := 0;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM team) THEN
        RAISE EXCEPTION 'Некорректное значение team_id';
    END IF;

    IF $2 < 2000 OR $2 > EXTRACT(YEAR FROM NOW()) THEN
        RAISE EXCEPTION 'Некорректное значение season';
    END IF;

    SELECT SUM(result.points) INTO total_points
    FROM result
    JOIN race ON result.race_id = race.id
    WHERE result.team_id = $1
    AND EXTRACT(YEAR FROM race.date) = $2;

    RETURN total_points;
END;
$$ LANGUAGE plpgsql;

SELECT team_points(1, 2000) AS total_points;
SELECT team_points(100, 2000) AS total_points;
SELECT team_points(1, 2024) AS total_points;






-- 5. Возвращает таблицу с пьедесталом по race_id (исключения)
CREATE OR REPLACE FUNCTION race_winners(race_id INT)
RETURNS TABLE (place BIGINT, driver_id INT, team_id INT) AS $$
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM race) THEN
        RAISE EXCEPTION 'Некорректное значение race_id';
    END IF;

    IF (SELECT status FROM race WHERE id = $1) = 'upcoming' THEN
        RAISE EXCEPTION 'Гонка еще не состоялась';
    END IF;

    IF (SELECT status FROM race WHERE id = $1) = 'cancelled' THEN
        RAISE EXCEPTION 'Гонка отменена';
    END IF;

    RETURN QUERY
    SELECT
        RANK() OVER (ORDER BY result.points DESC) AS place,
        result.driver_id,
        result.team_id
    FROM result
    INNER JOIN driver ON result.driver_id = driver.id
    INNER JOIN team ON result.team_id = team.id
    WHERE result.race_id = $1
    AND result.status = 'finished'
    ORDER BY result.points DESC
    LIMIT 3;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM race_winners(100);
SELECT * FROM race_winners(1306);
SELECT * FROM race_winners(1305);
SELECT * FROM race_winners(21);




-- 6. Средняя цена билетов для каждого из 3 классов по race_id (ветвление)
CREATE OR REPLACE FUNCTION tickets_avg_price(race_id INT)
RETURNS TABLE(class INT, avg_price NUMERIC) AS $$
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM race) THEN
        RAISE EXCEPTION 'Некорректное значение race_id';
    END IF;

    RETURN QUERY
    SELECT
        CASE
            WHEN (seat->>'Sector')::INT BETWEEN 1 AND 5 THEN 1
            WHEN (seat->>'Sector')::INT BETWEEN 6 AND 15 THEN 2
            ELSE 3
        END AS class,
        AVG(price) AS avg_price
    FROM ordering
    JOIN ticket ON ordering.id = ticket.ordering_id
    WHERE ordering.race_id = $1
    GROUP BY class;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM tickets_avg_price(1);
SELECT * FROM tickets_avg_price(1306);



-- 6'. Вариант с циклом
CREATE OR REPLACE FUNCTION ticket_avgprice(race_id INT) RETURNS TABLE (class INT, avg_price NUMERIC) AS $$
DECLARE
    class_1_cnt INT := 0;
    class_2_cnt INT := 0;
    class_3_cnt INT := 0;
    class_1_sum NUMERIC := 0;
    class_2_sum NUMERIC := 0;
    class_3_sum NUMERIC := 0;
    orders RECORD;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM race) THEN
        RAISE EXCEPTION 'Некорректное значение race_id';
    END IF;

    FOR orders IN
        SELECT (ticket.seat->>'Sector')::INT AS sector, ticket.price
        FROM ordering
        JOIN ticket ON ordering.id = ticket.ordering_id
        WHERE ordering.race_id = $1
    LOOP
        IF orders.sector BETWEEN 1 AND 5 THEN
            class_1_sum := class_1_sum + orders.price;
            class_1_cnt := class_1_cnt + 1;
        ELSIF orders.sector BETWEEN 6 AND 15 THEN
            class_2_sum := class_2_sum + orders.price;
            class_2_cnt := class_2_cnt + 1;
        ELSIF orders.sector BETWEEN 16 AND 25 THEN
            class_3_sum := class_3_sum + orders.price;
            class_3_cnt := class_3_cnt + 1;
        END IF;
    END LOOP;

    RETURN QUERY SELECT 1 AS class, class_1_sum / class_1_cnt AS avg_price
    UNION ALL
    SELECT 2 AS class, class_2_sum / class_2_cnt AS avg_price
    UNION ALL
    SELECT 3 AS class, class_3_sum / class_3_cnt AS avg_price;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM ticket_avgprice(1);



-- 7. Все гонки на трассе по track_id (циклы и курсоры)
CREATE OR REPLACE FUNCTION races_on_track(track_id INT) 
RETURNS TABLE (race_id INT, race_status RS3, race_date DATE) AS $$
DECLARE
    race_cursor CURSOR FOR
        SELECT id, status, date
        FROM race
        WHERE race.track_id = $1;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM track) THEN
        RAISE EXCEPTION 'Некорректное значение track_id';
    END IF;

    OPEN race_cursor;
    LOOP
        FETCH race_cursor INTO race_id, race_status, race_date;
        EXIT WHEN NOT FOUND;
        RETURN NEXT;
    END LOOP;
    CLOSE race_cursor;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM races_on_track(1);



-- 8. Все бронирования с количеством билетов и ценой по customer_id (цикл и курсор)
CREATE OR REPLACE FUNCTION customer_orders_info(customer_id INT)
RETURNS TABLE (ordering_id INT, num_tickets INT, price NUMERIC) AS $$
DECLARE
    order_id INT;
    total_tickets INT;
    total_price NUMERIC;
    order_cursor CURSOR FOR
        SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
        FROM ordering
        INNER JOIN ticket ON ordering.id = ticket.ordering_id
        WHERE ordering.customer_id = $1
        GROUP BY ordering.id;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM customer) THEN
        RAISE EXCEPTION 'Некорректное значение customer_id';
    END IF;

    FOR order_rec IN order_cursor
    LOOP
        ordering_id := order_rec.id;
        num_tickets := order_rec.count;
        price := order_rec.sum;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM customer_orders_info(1);



-- 8'. Без курсора - то же время
CREATE OR REPLACE FUNCTION orders_info(customer_id INT)
RETURNS TABLE (ordering_id INT, num_tickets INT, price NUMERIC) AS $$
DECLARE
    order_rec RECORD;
    total_tickets INT := 0;
    total_price NUMERIC := 0;
BEGIN
    IF $1 < 1 OR $1 > (SELECT MAX(id) FROM customer) THEN
        RAISE EXCEPTION 'Некорректное значение customer_id';
    END IF;
    
    FOR order_rec IN (
        SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
        FROM ordering
        INNER JOIN ticket ON ordering.id = ticket.ordering_id
        WHERE ordering.customer_id = $1
        GROUP BY ordering.id
    )
    LOOP
        ordering_id := order_rec.id;
        num_tickets := order_rec.count;
        price := order_rec.sum;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM orders_info(1);