-- +++ 1. race_id -> количество отмененный заказов      (ordering)
-- +++ 2. сustomer_id -> все билеты и price             (ordering + ticket)     
-- +++ 3. race_id -> Средняя цена билета 1го класса     (ordering + ticket)
-- +++ 4. Все билеты 1го сектора                        (ticket)
-- +++ 5. Поиск клиентов по номеру БК                   (customer)              (cards)                             массив
-- +++ 6. Все билеты с доступом к VIP ложе              (ticket)                description                         текстовый поиск
-- +++ 7. DELETE UPDATE INSERT в ticket 

-- B-дерево     по умолчанию
-- HASH         для равенств, нет сравнений
-- GIN          для полнотекстового поиска и поиска по массивам или JSON-данным
-- BRIN
-- GiST
-- SP-GiST

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1. race_id -> количество отмененный заказов (1 таблица)
SELECT COUNT(*) AS cancelled_race_cnt
FROM ordering
WHERE race_id = 100 AND status = 'cancelled';

-- A) Получить план выполнения запроса без использования индексов.
EXPLAIN SELECT COUNT(*)
FROM ordering
WHERE race_id = 100 AND status = 'cancelled';

-- Finalize Aggregate  (cost=409003.72..409003.73 rows=1 width=8)                              Полное агрегирование данных
--     ->  Gather  (cost=409003.51..409003.72 rows=2 width=8)                                  объединение результатов всех процессов
--     Workers Planned: 2                                                                      количество параллельных процессов
--         ->  Partial Aggregate  (cost=408003.51..408003.52 rows=1 width=8)                   агрегирование данных (подсчет строк)
--             ->  Parallel Seq Scan on ordering  (cost=0.00..408001.30 rows=882 width=0)   ^  сканирование
--                 Filter: ((race_id = 1) AND (status = 'cancelled'::os3))                  |  фильтрация

-- (cost=409003.72..409003.73 rows=1 width=8) 
--     cost - запрос оценивается в диапазоне от 409003.72 до 409003.73 единиц стоимости
-- стоимость запуска - время, которое проходит, прежде чем начнётся этап вывода данных, например для сортирующего узла это время сортировки)
-- Приблизительная общая стоимость - она вычисляется в предположении, что узел плана выполняется до конца, то есть возвращает все доступные строки)
--     rows - оценка количества строк, которые будут возвращены этой операцией
--     width - оценка размера среднего размера строки данных, в байтах, которую вернет данная операция










-- B) Получить статистику (IO и Time) выполнения запроса без использования индексов
EXPLAIN (ANALYZE) SELECT COUNT(*)
FROM ordering
WHERE race_id = 1 AND status = 'cancelled';

-- Finalize Aggregate  (cost=409003.72..409003.73 rows=1 width=8) (actual time=1310.411..1314.823 rows=1 loops=1)
--     ->  Gather  (cost=409003.51..409003.72 rows=2 width=8) (actual time=1310.302..1314.817 rows=3 loops=1)
--     Workers Planned: 2
--     Workers Launched: 2
--     ->  Partial Aggregate  (cost=408003.51..408003.52 rows=1 width=8) (actual time=1207.793..1207.794 rows=1 loops=3) --вероятно было разделение данных на 3 части
--         ->  Parallel Seq Scan on ordering  (cost=0.00..408001.30 rows=882 width=0) (actual time=613.903..1207.788 rows=2 loops=3)
--             Filter: ((race_id = 1) AND (status = 'cancelled'::os3))
--             Rows Removed by Filter: 11668416 --(не подходят)
-- Planning Time: 0.063 ms
-- Execution Time: 1440.722 ms


-- (cost=409003.72..409003.73 rows=1 width=8) (actual time=1310.411..1314.823 rows=1 loops=1)
-- actual time - фактическое время выполнения этой операции в миллисекундах
-- rows - количество строк, возвращенных этой операцией
-- loops - количество выполнений этой операции (обычно 1, если запрос не использует циклы)











-- C) Создать нужные индексы, позволяющие ускорить запрос.
-- CREATE INDEX idx_race__id ON ordering(race_id); -- 18s 
-- CREATE INDEX idx_status ON ordering(status); -- 15s   Второй не нужен!!!

-- !!!
-- Но для этого потребуется обойти оба индекса, так что это не обязательно будет выгоднее, чем просто просмотреть один индекс,
-- а второе условие обработать как фильтр. Измените диапазон и вы увидите, как это повлияет на план.
-- !!!

-- 2 индекса:
-- Aggregate  (cost=39872.60..39872.61 rows=1 width=8) (actual time=118.470..118.471 rows=1 loops=1)
--     Операция сканирования таблицы с использованием битовых карт (Bitmap Heap Scan)
--     Слово «bitmap» (битовая карта) в имени узла обозначает механизм, выполняющий сортировку)
--     ->  Bitmap Heap Scan on ordering  (cost=32081.63..39867.31 rows=2116 width=0) (actual time=118.367..118.410 rows=5 loops=1)
--         Условие повторной проверки (Recheck Condition)
--         Recheck Cond: ((race_id = 1) AND (status = 'cancelled'::os3))
--         Количество строк, которые были исключены из результатов после повторной проверки
--         Rows Removed by Index Recheck: 62
--         Количество блоков кучи (Heap Blocks), используемых операцией сканирования таблицы
--         Heap Blocks: exact=67
--         Операция AND битовых карт, которая объединяет результаты из индексных сканов
--         ->  BitmapAnd  (cost=32081.63..32081.63 rows=2116 width=0) (actual time=118.268..118.269 rows=0 loops=1)
--             Операция сканирования индекса idx__race_id на поле race_id
--             ->  Bitmap Index Scan on idx_race__id  (cost=0.00..278.59 rows=25354 width=0) (actual time=0.016..0.016 rows=88 loops=1)
--                 Index Cond: (race_id = 1)
--             Операция сканирования индекса idx_status на поле status.
--             ->  Bitmap Index Scan on idx_status  (cost=0.00..31801.73 rows=2921772 width=0) (actual time=117.405..117.406 rows=2915403 loops=1)
--                 Index Cond: (status = 'cancelled'::os3)
-- Planning Time: 0.093 ms
-- Execution Time: 82.975 ms (было 1314.845 ms) -> быстрее в 17 раз!!!
-- Не использует параллельные процессы


-- 1 индекс
-- Aggregate  (cost=70453.32..70453.33 rows=1 width=8) (actual time=0.163..0.163 rows=1 loops=1)
--     ->  Bitmap Heap Scan on ordering  (cost=279.12..70448.03 rows=2116 width=0) (actual time=0.034..0.086 rows=5 loops=1)
--         Recheck Cond: (race_id = 1)
--         Filter: (status = 'cancelled'::os3)
--         Rows Removed by Filter: 83
--         Heap Blocks: exact=88
--         ->  Bitmap Index Scan on idx_race__id  (cost=0.00..278.59 rows=25354 width=0) (actual time=0.015..0.016 rows=88 loops=1)
--             Index Cond: (race_id = 1)                    !!!(Именно сначала ИНДЕСКАЦИЯ, а потом ФИЛЬТРАЦИЯ)!!!
-- Planning Time: 0.076 ms
-- Execution Time: 0.108 ms (в 13 тыс раз быстрее!!!)


-- CREATE INDEX aaa ON ordering USING HASH (race_id); - HASH дает примерно то же время


CREATE INDEX idx_race_status ON ordering (race_id, status); --20s

-- Без индексов - 1400 ms
-- 2 индекса - 82 ms
-- 1 индекс - 0.1 ms
-- 1 индекс hash - 0.1m s
-- 1 индекс с 2 полями - 0.032 ms






-- D) Получить план выполнения запроса с использованием индексов и сравнить с первоначальным планом.
EXPLAIN SELECT COUNT(*)
FROM ordering
WHERE race_id = 100 AND status = 'cancelled';

-- Aggregate  (cost=64.05..64.06 rows=1 width=8)
--     ->  Index Only Scan using idx_race_status on ordering  (cost=0.44..58.76 rows=2116 width=0)
--         Index Cond: ((race_id = 100) AND (status = 'cancelled'::os3))





-- E) Получить статистику выполнения запроса с использованием индексов и сравнить с первоначальной статистикой
EXPLAIN (ANALYZE) SELECT COUNT(*)
FROM ordering
WHERE race_id = 1 AND status = 'cancelled';

-- Aggregate  (cost=64.05..64.06 rows=1 width=8) (actual time=0.024..0.025 rows=1 loops=1)
--     ->  Index Only Scan using idx_race_status on ordering  (cost=0.44..58.76 rows=2116 width=0) (actual time=0.017..0.020 rows=5 loops=1);
--     Index Cond: ((race_id = 1) AND (status = 'cancelled'::os3))
--     Heap Fetches: 0 (не было извлечения данных из физической кучи, все поместилось в памяти)
-- Planning Time: 0.074 ms
-- Execution Time: 0.032 ms (!!! в 45 тыс раз быстрее !!!)


DROP INDEX IF EXISTS idx_race_status;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



-- 2. Основная информация о заказах клиента (2 таблицы)

SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.customer_id = 1 AND ordering.status = 'paid'
GROUP BY ordering.id; --18s


-- A) Получить план выполнения запроса без использования индексов
EXPLAIN SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.customer_id = 1 AND ordering.status = 'paid'
GROUP BY ordering.id;

-- Finalize GroupAggregate  (cost=4975360.47..4975366.34 rows=20 width=44)
--     Group Key: ordering.id
--     ->  Gather Merge  (cost=4975360.47..4975365.69 rows=40 width=44)
--         Workers Planned: 2
--         ->  Partial GroupAggregate  (cost=4974360.45..4974361.05 rows=20 width=44)
--             Group Key: ordering.id
--             ->  Sort  (cost=4974360.45..4974360.53 rows=35 width=13)
--                 Sort Key: ordering.id
--                 ->  Parallel Hash Join  (cost=408000.92..4974359.55 rows=35 width=13)
--                     Hash Cond: (ticket.ordering_id = ordering.id)
--                     ->  Parallel Seq Scan on ticket  (cost=0.00..4408410.20 rows=60170820 width=13)
--                     ->  Parallel Hash  (cost=408000.83..408000.83 rows=8 width=4)
--                         ->  Parallel Seq Scan on ordering  (cost=0.00..408000.83 rows=8 width=4)
--                             Filter: ((customer_id = 1) AND (status = 'paid'::os3))


-- B) Получить статистику (IO и Time) выполнения запроса без использования индексов
EXPLAIN (ANALYZE) SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.customer_id = 1 AND ordering.status = 'paid'
GROUP BY ordering.id;

-- Finalize GroupAggregate  (cost=4975360.47..4975366.34 rows=20 width=44) (actual time=19356.909..19363.392 rows=2 loops=1)
--     Group Key: ordering.id
--     ->  Gather Merge  (cost=4975360.47..4975365.69 rows=40 width=44) (actual time=19356.901..19363.382 rows=2 loops=1)
--         Workers Planned: 2
--         Workers Launched: 2
--         ->  Partial GroupAggregate  (cost=4974360.45..4974361.05 rows=20 width=44) (actual time=19310.186..19310.188 rows=1 loops=3)
--             Group Key: ordering.id
--             ->  Sort  (cost=4974360.45..4974360.53 rows=35 width=13) (actual time=19310.180..19310.182 rows=2 loops=3)
--                 Sort Key: ordering.id
--                 Используемый метод сортировки и объем памяти, необходимый для выполнения операции.
--                 Sort Method: quicksort  Memory: 25kB
--                 Worker 0:  Sort Method: quicksort  Memory: 25kB
--                 Worker 1:  Sort Method: quicksort  Memory: 25kB
--                 ->  Parallel Hash Join  (cost=408000.92..4974359.55 rows=35 width=13) (actual time=15697.084..19310.077 rows=2 loops=3)
--                     Hash Cond: (ticket.ordering_id = ordering.id)
--                     ->  Parallel Seq Scan on ticket  (cost=0.00..4408410.20 rows=60170820 width=13) (actual time=5.667..13688.153 rows=48136656 loops=3)
--                     ->  Parallel Hash  (cost=408000.83..408000.83 rows=8 width=4) (actual time=1696.627..1696.627 rows=1 loops=3)
--                         Информация о количестве батчей в хэше и использованной памяти
--                         Buckets: 1024  Batches: 1  Memory Usage: 40kB
--                         ->  Parallel Seq Scan on ordering  (cost=0.00..408000.83 rows=8 width=4) (actual time=1115.625..1696.590 rows=1 loops=3)
--                             Filter: ((customer_id = 1) AND (status = 'paid'::os3))
--                             Rows Removed by Filter: 11668417
-- Planning Time: 0.220 ms
-- Execution Time: 19363.448 ms




-- C) Создать нужные индексы, позволяющие ускорить запрос.
CREATE INDEX idx_customer_status ON ordering(customer_id, status); --14s
CREATE INDEX idx_ticket_ordering_id ON ticket(ordering_id); --1m20s




-- D) Получить план выполнения запроса с использованием индексов и сравнить с первоначальным планом.
EXPLAIN SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.customer_id = 1 AND ordering.status = 'paid'
GROUP BY ordering.id;

-- GroupAggregate  (cost=1339.24..1340.32 rows=20 width=44)
--     Group Key: ordering.id
--     ->  Sort  (cost=1339.24..1339.44 rows=83 width=13)
--         Sort Key: ordering.id
--         ->  Nested Loop  (cost=1.01..1336.59 rows=83 width=13)
--             ->  Index Scan using idx_customer_status on ordering  (cost=0.44..42.09 rows=20 width=4)
--                 Index Cond: ((customer_id = 1) AND (status = 'paid'::os3))
--             ->  Index Scan using idx_ticket_ordering_id on ticket  (cost=0.57..60.30 rows=442 width=13)
--                 Index Cond: (ordering_id = ordering.id)


-- E) Получить статистику выполнения запроса с использованием индексов и сравнить с первоначальной статистикой

EXPLAIN (ANALYZE) SELECT ordering.id, COUNT(ticket.id), SUM(ticket.price)
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.customer_id = 1 AND ordering.status = 'paid'
GROUP BY ordering.id;

-- GroupAggregate  (cost=1339.24..1340.32 rows=20 width=44) (actual time=0.054..0.056 rows=2 loops=1)
--     Group Key: ordering.id
--     ->  Sort  (cost=1339.24..1339.44 rows=83 width=13) (actual time=0.048..0.049 rows=7 loops=1)
--         Sort Key: ordering.id
--         Sort Method: quicksort  Memory: 25kB
--         ->  Nested Loop  (cost=1.01..1336.59 rows=83 width=13) (actual time=0.034..0.039 rows=7 loops=1)
--             ->  Index Scan using idx_customer_status on ordering  (cost=0.44..42.09 rows=20 width=4) (actual time=0.013..0.014 rows=2 loops=1)
--                 Index Cond: ((customer_id = 1) AND (status = 'paid'::os3))
--             ->  Index Scan using idx_ticket_ordering_id on ticket  (cost=0.57..60.30 rows=442 width=13) (actual time=0.010..0.011 rows=4 loops=2)
--                 Index Cond: (ordering_id = ordering.id)
-- Planning Time: 0.215 ms
-- Execution Time: 0.049 ms

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 3. race_id -> Средняя цена билета 1го сектора (2 таблицы, jsonb) (пример выбора индексов)
SELECT ROUND(AVG(price), 2) AS avg_price
FROM ticket
INNER JOIN ordering ON ticket.ordering_id = ordering.id
WHERE ordering.race_id = 1 AND seat @> '{"Sector": 1}';   -- 1m6s




-- A) Получить план выполнения запроса без использования индексов.
EXPLAIN SELECT ROUND(AVG(price), 2) AS avg_price
FROM ticket
INNER JOIN ordering ON ticket.ordering_id = ordering.id
WHERE ordering.race_id = 1 AND seat @> '{"Sector": 1}';

-- Finalize Aggregate  (cost=4933087.12..4933087.13 rows=1 width=32)
--     ->  Gather  (cost=4933086.90..4933087.11 rows=2 width=32)
--     Workers Planned: 2
--     ->  Partial Aggregate  (cost=4932086.90..4932086.91 rows=1 width=32)
--         Объединение данных из таблицы ticket и ordering
--         ->  Parallel Hash Join  (cost=371669.07..4932085.80 rows=436 width=5)
--             Hash Cond: (ticket.ordering_id = ordering.id)
--             ->  Parallel Seq Scan on ticket  (cost=0.00..4558837.25 rows=601708 width=9)
--                 Filter: (seat @> '{"Sector": 1}'::jsonb)
--             Строится хэш-таблица для использования в хэш-соединении
--             ->  Parallel Hash  (cost=371537.02..371537.02 rows=10564 width=4)
--                 ->  Parallel Seq Scan on ordering  (cost=0.00..371537.02 rows=10564 width=4)
--                     Filter: (race_id = 1)


-- Cтарый
-- Finalize Aggregate  (cost=5383577.98..5383578.00 rows=1 width=32)
--     ->  Gather  (cost=5383577.76..5383577.97 rows=2 width=32)
--     Workers Planned: 2
--     ->  Partial Aggregate  (cost=5382577.76..5382577.77 rows=1 width=32)
--         ->  Parallel Hash Join  (cost=371669.07..5382577.21 rows=218 width=5)
--             Объединение данных из таблицы ticket и ordering.
--             Hash Cond: (ticket.ordering_id = ordering.id)
--             ->  Parallel Seq Scan on ticket  (cost=0.00..5010118.40 rows=300854 width=9)
--                 Filter: (((seat -> 'Sector'::text) >= '16'::jsonb) AND ((seat -> 'Sector'::text) <= '25'::jsonb))
--             Строится хэш-таблица для использования в хэш-соединении
--             ->  Parallel Hash  (cost=371537.02..371537.02 rows=10564 width=4)
--                 ->  Parallel Seq Scan on ordering  (cost=0.00..371537.02 rows=10564 width=4)
--                     Filter: (race_id = 1)


-- Старый вывод с INTEGER
-- Finalize Aggregate  (cost=5985286.18..5985286.20 rows=1 width=32)
--     ->  Gather  (cost=5985285.96..5985286.17 rows=2 width=32)
--         Workers Planned: 2
--         ->  Partial Aggregate  (cost=5984285.96..5984285.97 rows=1 width=32)
--             ->  Parallel Hash Join  (cost=371669.07..5984285.41 rows=218 width=5)
--                 Объединение данных из таблицы ticket и ordering.
--                 Hash Cond: (ticket.ordering_id = ordering.id)
--                 ->  Parallel Seq Scan on ticket  (cost=0.00..5611826.60 rows=300854 width=9)
--                     Filter: ((((seat ->> 'Sector'::text))::integer >= 16) AND (((seat ->> 'Sector'::text))::integer <= 25))
--                 Строится хэш-таблица для использования в хэш-соединении
--                 ->  Parallel Hash  (cost=371537.02..371537.02 rows=10564 width=4)
--                     ->  Parallel Seq Scan on ordering  (cost=0.00..371537.02 rows=10564 width=4)
--                         Filter: (race_id = 1)




-- B) Получить статистику (IO и Time) выполнения запроса без использования индексов
EXPLAIN (ANALYZE) SELECT ROUND(AVG(price), 2) AS avg_price
FROM ticket
INNER JOIN ordering ON ticket.ordering_id = ordering.id
WHERE ordering.race_id = 1 AND seat @> '{"Sector": 1}'; --44s

-- Finalize Aggregate  (cost=4933087.12..4933087.13 rows=1 width=32) (actual time=44563.982..44575.473 rows=1 loops=1)
--     ->  Gather  (cost=4933086.90..4933087.11 rows=2 width=32) (actual time=44563.722..44575.455 rows=3 loops=1)
--         Workers Planned: 2
--         Workers Launched: 2
--         ->  Partial Aggregate  (cost=4932086.90..4932086.91 rows=1 width=32) (actual time=44508.074..44508.077 rows=1 loops=3)
--             ->  Parallel Hash Join  (cost=371669.07..4932085.80 rows=436 width=5) (actual time=28638.523..44508.062 rows=3 loops=3)
--                 Hash Cond: (ticket.ordering_id = ordering.id)
--                 ->  Parallel Seq Scan on ticket  (cost=0.00..4558837.25 rows=601708 width=9) (actual time=2.163..42424.299 rows=1925367 loops=3)
--                     Filter: (seat @> '{"Sector": 1}'::jsonb)
--                     Rows Removed by Filter: 46211289
--                 ->  Parallel Hash  (cost=371537.02..371537.02 rows=10564 width=4) (actual time=1633.115..1633.115 rows=29 loops=3)
--                     Разделение хэш-таблицы на 32768 частей, загрузка 1 батчем в ОЗУ (352kb)
--                     Buckets: 32768  Batches: 1  Memory Usage: 352kB
--                     ->  Parallel Seq Scan on ordering  (cost=0.00..371537.02 rows=10564 width=4) (actual time=64.522..1632.869 rows=29 loops=3)
--                         Filter: (race_id = 1)
--                         Rows Removed by Filter: 11668388
-- Planning Time: 0.248 ms
-- Execution Time: 44575.525 ms







-- C) Создать нужные индексы, позволяющие ускорить запрос.
CREATE INDEX idx_race ON ordering(race_id); -- 15s
-- CREATE INDEX idx_seat ON ticket USING GIN (seat);
-- CREATE INDEX idx_seat ON ticket USING GIN ((seat->'Sector'));
CREATE INDEX idx_seat ON ticket USING GIN (seat jsonb_path_ops);
CREATE INDEX idx_ticket_ordering_id ON ticket(ordering_id); --1m57s приводит к филтрации seat, но важен



-- D) Получить план выполнения запроса с использованием индексов и сравнить с первоначальным планом.

EXPLAIN SELECT ROUND(AVG(price), 2) AS avg_price
FROM ordering
INNER JOIN ticket ON ordering.id = ticket.ordering_id
WHERE ordering.race_id = 1 AND seat @> '{"Sector": 1}';

-- Finalize Aggregate  (cost=698530.04..698530.05 rows=1 width=32)
--     ->  Gather  (cost=698529.81..698530.02 rows=2 width=32)
--         Workers Planned: 2
--         ->  Partial Aggregate  (cost=697529.81..697529.82 rows=1 width=32)
--             ->  Nested Loop  (cost=285.50..697528.72 rows=436 width=5)
--                 Операция сканирования таблицы с использованием битовых карт (Bitmap Heap Scan)
--                 ->  Parallel Bitmap Heap Scan on ordering  (cost=284.93..70205.58 rows=10564 width=4)
--                     Recheck Cond: (race_id = 1)
--                     ->  Bitmap Index Scan on idx_race  (cost=0.00..278.59 rows=25354 width=0)
--                         Index Cond: (race_id = 1)
--                 ->  Index Scan using idx_ticket_ordering_id on ticket  (cost=0.57..59.34 rows=4 width=9)
--                     Index Cond: (ordering_id = ordering.id)
--                     Filter: (seat @> '{"Sector": 1}'::jsonb)


-- Без использования idx_ticket_ordering_id
-- Finalize Aggregate  (cost=3628591.04..3628591.05 rows=1 width=32)
--     ->  Gather  (cost=3628590.81..3628591.02 rows=2 width=32)
--         Workers Planned: 2
--         ->  Partial Aggregate  (cost=3627590.81..3627590.82 rows=1 width=32)
--             ->  Parallel Hash Join  (cost=94901.40..3627589.72 rows=436 width=5)
--                 Hash Cond: (ticket.ordering_id = ordering.id)
--                 ->  Parallel Bitmap Heap Scan on ticket  (cost=24563.77..3555672.61 rows=601708 width=9)
--                     Recheck Cond: (seat @> '{"Sector": 1}'::jsonb)
--   !!!               ->  Bitmap Index Scan on idx_seat  (cost=0.00..24202.75 rows=1444100 width=0)
--                         Index Cond: (seat @> '{"Sector": 1}'::jsonb)
--                 ->  Parallel Hash  (cost=70205.58..70205.58 rows=10564 width=4)
--                     ->  Parallel Bitmap Heap Scan on ordering  (cost=284.93..70205.58 rows=10564 width=4)
--                         Recheck Cond: (race_id = 1)
--                         ->  Bitmap Index Scan on idx_race  (cost=0.00..278.59 rows=25354 width=0)
--                             Index Cond: (race_id = 1)


-- E) Получить статистику выполнения запроса с использованием индексов и сравнить с первоначальной статистикой

EXPLAIN (ANALYZE) SELECT ROUND(AVG(price), 2) AS avg_price
FROM ticket
INNER JOIN ordering ON ticket.ordering_id = ordering.id
WHERE ordering.race_id = 1 AND seat @> '{"Sector": 1}';

-- Finalize Aggregate  (cost=698530.04..698530.05 rows=1 width=32) (actual time=71.053..75.454 rows=1 loops=1)
--     ->  Gather  (cost=698529.81..698530.02 rows=2 width=32) (actual time=24.639..75.439 rows=3 loops=1)
--         Workers Planned: 2
--         Workers Launched: 2
--         ->  Partial Aggregate  (cost=697529.81..697529.82 rows=1 width=32) (actual time=8.121..8.122 rows=1 loops=3)
--             ->  Nested Loop  (cost=285.50..697528.72 rows=436 width=5) (actual time=3.055..8.116 rows=3 loops=3)
--                 ->  Parallel Bitmap Heap Scan on ordering  (cost=284.93..70205.58 rows=10564 width=4) (actual time=0.120..0.579 rows=29 loops=3)
--                     Recheck Cond: (race_id = 1)
--                     Heap Blocks: exact=88
--                     ->  Bitmap Index Scan on idx_race  (cost=0.00..278.59 rows=25354 width=0) (actual time=0.273..0.273 rows=88 loops=1)
--                         Index Cond: (race_id = 1)
--                 ->  Index Scan using idx_ticket_ordering_id on ticket  (cost=0.57..59.34 rows=4 width=9) (actual time=0.255..0.255 rows=0 loops=88)
--                     Index Cond: (ordering_id = ordering.id)
--                     Filter: (seat @> '{"Sector": 1}'::jsonb)
--                     Rows Removed by Filter: 5
-- Planning Time: 0.225 ms
-- Execution Time: 65.509 ms

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Все билеты 1го сектора (jsonb)

SELECT indexname 
FROM pg_indexes 
WHERE indexname LIKE 'idx_%';

DROP INDEX IF EXISTS idx_seat;

EXPLAIN (ANALYZE) SELECT * 
FROM ticket 
WHERE seat @> '{"Sector": 1}';
-- Gather  (cost=1000.00..4704247.25 rows=1444100 width=124) (actual time=0.550..26172.189 rows=5776101 loops=1)
--     Workers Planned: 2
--     Workers Launched: 2
--     ->  Parallel Seq Scan on ticket  (cost=0.00..4558837.25 rows=601708 width=124) (actual time=5.951..24463.860 rows=1925367 loops=3)
--         Filter: (seat @> '{"Sector": 1}'::jsonb)
--         Rows Removed by Filter: 46211289
-- Planning Time: 0.060 ms
-- Execution Time: 297372.310 ms




-- CREATE INDEX idx_seat ON ticket USING GIN (seat);
-- CREATE INDEX idx_seat ON ticket USING GIN ((seat->'Sector'));
CREATE INDEX idx_seat ON ticket USING GIN (seat jsonb_path_ops); -- !!!

ANALYSE ticket;
ANALYSE idx_seat;


EXPLAIN (ANALYZE) SELECT *
FROM ticket
WHERE seat @> '{"Sector": 1}';
-- Gather  (cost=14603.77..3690122.61 rows=1444100 width=124) (actual time=679.196..26405.177 rows=5776101 loops=1)
--     Workers Planned: 2
--     Workers Launched: 2
--     ->  Parallel Bitmap Heap Scan on ticket  (cost=13603.77..3544712.61 rows=601708 width=124) (actual time=622.202..26009.385 rows=1925367 loops=3)
--         Recheck Cond: (seat @> '{"Sector": 1}'::jsonb)
--         Rows Removed by Index Recheck: 12818875
--         Heap Blocks: exact=10058 lossy=383714
--         ->  Bitmap Index Scan on idx_seat  (cost=0.00..13242.75 rows=1444100 width=0) (actual time=669.258..669.258 rows=5776101 loops=1)
--             Index Cond: (seat @> '{"Sector": 1}'::jsonb)
-- Planning Time: 0.223 ms
-- Execution Time: 27493.820 ms (в 11 раз быстрее)



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 5. Поиск клиентов по номеру БК 

SELECT *
FROM customer
WHERE bank_cards @> '{"3518543660860808"}';


EXPLAIN (ANALYZE) SELECT *
FROM customer
WHERE bank_cards @> '{"3518543660860808"}';

-- Gather  (cost=1000.00..275430.11 rows=667 width=147) (actual time=0.378..1534.488 rows=221 loops=1)
--     Workers Planned: 2
--     Workers Launched: 2
--     ->  Parallel Seq Scan on customer  (cost=0.00..274363.41 rows=278 width=147) (actual time=12.284..1482.872 rows=74 loops=3)
--         Filter: (bank_cards @> '{3518543660860808}'::character varying[])
--         Rows Removed by Filter: 3333260
-- Planning Time: 0.092 ms
-- Execution Time: 1534.557 ms


CREATE INDEX idx_cards ON customer USING GIN(bank_cards); --2m 


EXPLAIN (ANALYZE) SELECT id, first_name, last_name
FROM customer
WHERE bank_cards @> '{"3518543660860808"}';

-- Bitmap Heap Scan on customer  (cost=25.17..2591.89 rows=667 width=18) (actual time=0.071..0.252 rows=221 loops=1)
--     Recheck Cond: (bank_cards @> '{3518543660860808}'::character varying[])
--     Heap Blocks: exact=221
--     ->  Bitmap Index Scan on idx_cards  (cost=0.00..25.00 rows=667 width=0) (actual time=0.044..0.044 rows=221 loops=1)
--         Index Cond: (bank_cards @> '{3518543660860808}'::character varying[])
-- Planning Time: 0.103 ms
-- Execution Time: 0.207 ms (в 8 тыс раз быстрее)


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 6. Все билеты с доступом к VIP ложе

EXPLAIN (ANALYZE)
SELECT id, ordering_id, description
FROM ticket
WHERE to_tsvector('english', description) @@ to_tsquery('english', 'VIP <-> (box | zone)');
-- Gather  (cost=1000.00..19600215.60 rows=7201 width=87) (actual time=1.809..936314.593 rows=10099399 loops=1)
--     Workers Planned: 2
--     Workers Launched: 2
--     ->  Parallel Seq Scan on ticket  (cost=0.00..19598495.50 rows=3000 width=87) (actual time=2.908..935181.710 rows=3366466 loops=3)
--         Filter: (to_tsvector('english'::regconfig, description) @@ '''vip'' <-> ( ''box'' | ''zone'' )'::tsquery)
--         Rows Removed by Filter: 44770190
-- Planning Time: 23.882 ms
-- Execution Time: 2093502.113 ms (35m)

CREATE INDEX idx_description ON ticket USING GIN(to_tsvector('english', description)); --37m


EXPLAIN (ANALYZE)
SELECT id, ordering_id, description
FROM ticket
WHERE to_tsvector('english', description) @@ to_tsquery('english', 'VIP <-> (box | zone)'); --15m

-- Bitmap Heap Scan on ticket (cost=23075.82..52811.74 rows=7202 width=87) (actual time=4042.526..940248.475 rows=10099399 loops=1)
--     Recheck Cond: (to_tsvector('english'::regconfig, description) @@ '''vip'' <-> ( ''box'' | ''zone'' )'::tsquery)
--     Rows Removed by Index Recheck: 58904407
--     Heap Blocks: exact=34273 lossy=1817035
--     ->  Bitmap Index Scan on idx_description  (cost=0.00..23074.02 rows=7202 width=0) (actual time=4032.905..4032.905 rows=10099399 loops=1)
--         Index Cond: (to_tsvector('english'::regconfig, description) @@ '''vip'' <-> ( ''box'' | ''zone'' )'::tsquery)
-- Planning Time: 0.093 ms
-- Execution Time: 837401.753 ms (14m)


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 7. Секционирование таблицы ticket

CREATE TABLE IF NOT EXISTS tickets_by_seasons (
    id SERIAL,
    ordering_id INT NOT NULL,
    seat JSONB NOT NULL,
    price NUMERIC NOT NULL,
    description TEXT,
    PRIMARY KEY (id, ordering_id),                          --необходимое требование включить ordering_id в PRIMARY KEY
    FOREIGN KEY (ordering_id) REFERENCES ordering(id)
) PARTITION BY RANGE (ordering_id);


CREATE TABLE IF NOT EXISTS tickets_2000 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (1) TO (132284);

CREATE TABLE IF NOT EXISTS tickets_2001 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (132284) TO (513854);

CREATE TABLE IF NOT EXISTS tickets_2002 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (513854) TO (1145645);

CREATE TABLE IF NOT EXISTS tickets_2003 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (1145645) TO (2026101);

CREATE TABLE IF NOT EXISTS tickets_2004 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (2026101) TO (3155366);

CREATE TABLE IF NOT EXISTS tickets_2005 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (3155366) TO (4564017);

CREATE TABLE IF NOT EXISTS tickets_2006 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (4564017) TO (6197371);

CREATE TABLE IF NOT EXISTS tickets_2007 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (6197371) TO (8080896);

CREATE TABLE IF NOT EXISTS tickets_2008 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (8080896) TO (10159441);

CREATE TABLE IF NOT EXISTS tickets_2009 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (10159441) TO (12250605);

CREATE TABLE IF NOT EXISTS tickets_2010 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (12250605) TO (14340312);

CREATE TABLE IF NOT EXISTS tickets_2011 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (14340312) TO (16472409);

CREATE TABLE IF NOT EXISTS tickets_2012 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (16472409) TO (18563348);

CREATE TABLE IF NOT EXISTS tickets_2013 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (18563348) TO (20654729);

CREATE TABLE IF NOT EXISTS tickets_2014 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (20654729) TO (22747057);

CREATE TABLE IF NOT EXISTS tickets_2015 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (22747057) TO (24842234);

CREATE TABLE IF NOT EXISTS tickets_2016 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (24842234) TO (26960017);

CREATE TABLE IF NOT EXISTS tickets_2017 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (26960017) TO (28840355);

CREATE TABLE IF NOT EXISTS tickets_2018 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (28840355) TO (30470353);

CREATE TABLE IF NOT EXISTS tickets_2019 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (30470353) TO (31850134);

CREATE TABLE IF NOT EXISTS tickets_2020 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (31850134) TO (32979544);

CREATE TABLE IF NOT EXISTS tickets_2021 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (32979544) TO (33861436);

CREATE TABLE IF NOT EXISTS tickets_2022 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (33861436) TO (34502557);

CREATE TABLE IF NOT EXISTS tickets_2023 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (34502557) TO (34878240);

CREATE TABLE IF NOT EXISTS tickets_2024 PARTITION OF tickets_by_seasons
    FOR VALUES FROM (34878240) TO (MAXVALUE);


-- Перенос данных
-- INSERT INTO tickets_by_seasons SELECT * FROM ticket; --32m

-- Создание индекса
-- CREATE INDEX idx_tbs_ord ON tickets_by_seasons (ordering_id); --1m20s

-- Выборка
EXPLAIN (ANALYZE) SELECT * FROM tickets_by_seasons WHERE ordering_id = 10;


-- Удаление билетов с давних сезонов
DROP TABLE tickets_2000;

-- Вставка новых билетов -> автоматически в tickets_2024
-- INSERT INTO tickets_by_seasons VALUES (145000000, 36000000, '{"Row": 37, "Gate": 1, "Seat": 13, "Sector": 1}', 10000, 'Ticket to Formula 1 with VIP box access, driver meet and greet, and a complimentary bar.')


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT * FROM pg_indexes WHERE indexname LIKE 'idx%';
DROP INDEX IF EXISTS idx_tbs_ord;