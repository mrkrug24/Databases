-- Пилоты с наибольшей суммой очков на данный момент
SELECT 
    d.first_name, 
    d.last_name, 
    SUM(r.points) AS total_points, 
    DATE_PART('year', age(CURRENT_DATE, d.birthday)) AS age, 
    d.citizenship
FROM driver d
JOIN result r ON d.id = r.driver_id
WHERE r.driver_status = 'finished'
GROUP BY d.id
HAVING SUM(r.points) = (
    SELECT MAX(total_points)
    FROM (
        SELECT SUM(points) AS total_points
        FROM result
        WHERE driver_status = 'finished'
        GROUP BY driver_id
    ) AS max_sum
)
ORDER BY d.last_name;


-- Команды с наибольшей суммой баллов своих пилотов
SELECT 
    t.team_name, 
    SUM(r.points) AS total_points, 
    COUNT(DISTINCT d.id) AS total_drivers,
    t.country
FROM team t
JOIN driver d ON t.id = d.team_id
JOIN result r ON d.id = r.driver_id
WHERE r.driver_status = 'finished'
GROUP BY t.team_name, t.country
HAVING SUM(r.points) = (
    SELECT MAX(total_points)
    FROM (
        SELECT SUM(points) AS total_points
        FROM result
        JOIN driver ON driver.id = result.driver_id
        WHERE result.driver_status = 'finished'
        GROUP BY driver.team_id
    ) AS max_sum
)
ORDER BY t.team_name;


-- Гонки, в которых с дистанции сошло наибольшее количество пилотов
SELECT t.city, r.date, COUNT(*) AS num_pilots
FROM result AS res
JOIN race AS r ON res.race_id = r.id
JOIN track AS t ON t.race_id = r.id
WHERE res.driver_status IN ('breakdown', 'accident')
GROUP BY t.city, r.date
ORDER BY num_pilots DESC
LIMIT 10;

----------------------------------------------------------------------------------------------------------

-- Технические проблемы: Перенос всех предстоящие гонок на неделю
UPDATE race
SET date = date + INTERVAL '7 days'
WHERE race_status = 'upcoming';


-- Дисквалификация: удаление команды Williams (ON DELETE CASCADE):
--> удаление записи в таблице team
--> удаление записей в таблице driver
--> удаление записей в таблице result
DELETE FROM team
WHERE team_name = 'Williams';


-- Отмена гонки: трассу Imola в Италии затопило (ON DELETE CASCADE):
--> удаление записи в таблице race
--> удаление записей в таблице track
--> удаление записей в таблице result
DELETE FROM race
WHERE id IN (
    SELECT race_id
    FROM track
    WHERE city = 'Imola'
);