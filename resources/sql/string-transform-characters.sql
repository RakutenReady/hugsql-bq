-- :name string-transform-select-character
-- :command :query
-- :result :many
-- Selects all the characters with a given name
SELECT * FROM :i:fqn WHERE name = :v:name
