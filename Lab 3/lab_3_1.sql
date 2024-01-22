DROP SCHEMA IF EXISTS f1 CASCADE;
CREATE SCHEMA f1;
SET search_path TO f1_lab3;

DROP TABLE IF EXISTS track;
DROP TYPE IF EXISTS RS;
DROP TABLE IF EXISTS race;
DROP TABLE IF EXISTS driver;
DROP TABLE IF EXISTS team;
DROP TYPE IF EXISTS DS;
DROP TABLE IF EXISTS result;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS ticket;
DROP TYPE IF EXISTS OS;
DROP TABLE IF EXISTS ordering;



--------------------------------------------------------------------------------------------------------------------------------
-- Создание таблицы трасс 
CREATE TABLE IF NOT EXISTS track (
    id SERIAL PRIMARY KEY,
    city VARCHAR(20) NOT NULL,
    len INT CONSTRAINT positive_length CHECK (len > 0),
    UNIQUE (city)
);

-- Тип данных для race_status
CREATE TYPE RS3 AS ENUM ('passed', 'cancelled', 'upcoming');

-- Создание таблицы гонок
CREATE TABLE IF NOT EXISTS race (
    id SERIAL PRIMARY KEY,
    track_id INT NOT NULL,
    status RS3 NOT NULL,
    date DATE NOT NULL,
    laps INT CONSTRAINT positive_num_laps CHECK (laps > 0) NOT NULL,
    FOREIGN KEY (track_id) REFERENCES track(id)
);

-- Создание таблицы пилотов
CREATE TABLE IF NOT EXISTS driver (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL
);

-- Создание таблицы команд
CREATE TABLE IF NOT EXISTS team (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(20) NOT NULL,
    country VARCHAR(20) NOT NULL,
    UNIQUE (team_name)
);

-- Тип данных для driver_status
CREATE TYPE DS3 AS ENUM ('finished', 'missed', 'breakdown', 'accident', 'disqualified', 'upcoming');

-- Создание таблицы результатов
CREATE TABLE IF NOT EXISTS result (
    team_id INT NOT NULL,
    driver_id INT NOT NULL,
    race_id INT NOT NULL,
    status DS3 NOT NULL,
    points INT CONSTRAINT positive_num_points CHECK (points >= 0),
    CONSTRAINT status_points CHECK ((status <> 'finished') OR (status = 'finished' AND points IS NOT NULL)),
    PRIMARY KEY (driver_id, race_id),
    FOREIGN KEY (team_id) REFERENCES team(id),
    FOREIGN KEY (driver_id) REFERENCES driver(id),
    FOREIGN KEY (race_id) REFERENCES race(id)
);


-- Создание таблицы покупателей
CREATE TABLE IF NOT EXISTS customer (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    bank_cards VARCHAR(20)[],
    contacts JSONB NOT NULL
);

-- Тип данных для order_status
CREATE TYPE OS3 AS ENUM ('paid', 'cancelled');


-- Создание таблицы бронирования
CREATE TABLE IF NOT EXISTS ordering (
    id SERIAL PRIMARY KEY,
    race_id INT NOT NULL,
    customer_id INT NOT NULL,
    status OS3 NOT NULL,
    FOREIGN KEY (race_id) REFERENCES race(id),
    FOREIGN KEY (customer_id) REFERENCES customer(id)
);


-- Создание таблицы билетов
CREATE TABLE IF NOT EXISTS ticket (
    id SERIAL PRIMARY KEY,
    ordering_id INT NOT NULL,
    seat JSONB NOT NULL,
    price NUMERIC NOT NULL,
    description TEXT,
    FOREIGN KEY (ordering_id) REFERENCES ordering(id)
);