--1. Вычисляет среднюю стоимость брони и окрюгляет до целого, с помощью приведения типов
SELECT CAST(AVG(total_amount) AS INTEGER) AS "average cost" FROM bookings;


--2. Выводит названия всех моделей самолетов и количество мест, соответствующее каждому классу--
SELECT a.model, s.fare_conditions, COUNT(*) FROM seats s
JOIN aircrafts a ON a.aircraft_code = s.aircraft_code
GROUP BY model, fare_conditions
ORDER BY model;


--3. Выводит имена пассажиров и основную информацию о них: рейс, класс, место, аэропоры вылета и прибыттия--
SELECT t.passenger_name, f.flight_no, f.scheduled_departure, 
tf.fare_conditions, s.seat_no, f.departure_airport, f.arrival_airport FROM tickets t
INNER JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
INNER JOIN flights f ON tf.flight_id = f.flight_id
INNER JOIN seats s ON f.aircraft_code = s.aircraft_code
ORDER BY passenger_name
LIMIT 100;


--4. Наиболее дешевые билеты из Москвы
SELECT airports.city, CAST(MIN(amount) AS INTEGER) 
FROM ticket_flights
JOIN flights ON ticket_flights.flight_id = flights.flight_id
JOIN airports ON flights.arrival_airport = airports.airport_code
WHERE flights.departure_airport = 'DME' OR flights.departure_airport = 'VKO' OR flights.departure_airport = 'SVO'
GROUP BY airports.city
ORDER BY airports.city;


--5. Наиболее продолжительный по времеми рейс
SELECT departure_city, arrival_city, duration FROM routes ORDER BY duration DESC LIMIT 1;

--6. Заполненность
SELECT a.model, s.fare_conditions, COUNT(*) FROM seats s
JOIN aircrafts a ON a.aircraft_code = s.aircraft_code
GROUP BY model, fare_conditions
ORDER BY model;


SELECT passenger_id, passenger_name, SUM(flights_v.scheduled_duration) AS duration FROM tickets
INNER JOIN ticket_flights USING(ticket_no)
INNER JOIN flights_v USING(flight_id)
GROUP BY passenger_id, passenger_name
ORDER BY duration DESC
LIMIT 1;


SELECT 
    flight_no, 
    departure_airport,
    a1.coordinates AS dep_coord,
    arrival_airport,
    a2.coordinates AS arr_coord,
    a1.coordinates<@>a2.coordinates AS distance
FROM routes
INNER JOIN airports AS a1 ON a1.airport_code = departure_airport
INNER JOIN airports AS a2 ON a2.airport_code = arrival_airport
ORDER BY distance DESC;


SELECT
    passenger_id,
    passenger_name,
    SUM(a1.coordinates<@>a2.coordinates) as sum_distance 
FROM tickets
INNER JOIN ticket_flights USING(ticket_no)
INNER JOIN flights_v USING(flight_id)
INNER JOIN airports AS a1 ON a1.airport_code = departure_airport
INNER JOIN airports AS a2 ON a2.airport_code = arrival_airport
GROUP BY passenger_id, passenger_name
ORDER BY sum_distance DESC;


SELECT
    passenger_id,
    passenger_name,
    SUM(a1.coordinates<@>a2.coordinates) as sum_distance 
FROM tickets
INNER JOIN ticket_flights USING(ticket_no)
INNER JOIN flights_v USING(flight_id)
INNER JOIN airports AS a1 ON a1.airport_code = departure_airport
INNER JOIN airports AS a2 ON a2.airport_code = arrival_airport
GROUP BY passenger_id, passenger_name
ORDER BY sum_distance DESC;