-- :name create-characters-table
-- :command :execute
-- :result :affected
-- Creates a characters table if not exists
CREATE TABLE IF NOT EXISTS :i:fqn (
    name STRING,
    email STRING,
    age INT64,
    weapons ARRAY<STRING>,
    details STRUCT<
        teams ARRAY<STRING>,
        friendly BOOLEAN,
        num_movies INT64
    >,
    dob DATE,
    superhero BOOLEAN,
    weight FLOAT64,
    code BYTES,
    net_worth NUMERIC,
    updated_at TIMESTAMP
)
-- :name insert-character
-- :command :execute
-- :result :affected
-- Insert a single character into the characters table
INSERT INTO :i:fqn VALUES (
    :v:name,
    :v:email,
    :v:age,
    :v:weapons,
    :v:details,
    :v:dob,
    :v:superhero,
    :v:weight,
    :v:code,
    :v:net_worth,
    :v:updated_at
)
-- :name insert-characters
-- :command :execute
-- :result :affected
-- Insert a multiple characters into the characters table
INSERT INTO :i:fqn (name, email, age, weapons, dob, superhero, weight, code, net_worth, updated_at)
VALUES :tuple*:characters
-- :name select-character
-- :command :query
-- :result :many
-- Selects all the characters with a given name
SELECT * FROM :i:fqn WHERE name = :v:name
-- :name drop-characters-table
-- :command :execute
-- :result :raw
-- Drops the characters table
DROP TABLE IF EXISTS :i:fqn
-- :name list-tables
-- :command :query
-- :result :many
-- Lists all the tables for a given dataset
SELECT * EXCEPT (is_typed) FROM `clojure_bq_test`.`INFORMATION_SCHEMA`.`TABLES`
-- :name select-columns
-- :command :query
-- :result :one
-- Test HugSQL data structures
SELECT :i*:columns FROM :i:fqn WHERE name IN (:v*:names)
