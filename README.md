# hugsql-bq

A HugSQL adapter for BigQuery.

## Usage

First things first - make sure to have your `GOOGLE_APPLICATION_CREDENTIALS` & `GOOGLE_CLOUD_PROJECT` environment variables set.

```clojure
(ns example
  (:require 
   [hugsql.adapter.hugsql-bq :as hugsql-bq]
   [hugsql.core :as hugsql])
  (:import
   (com.google.cloud.bigquery BigQueryOptions$Builder BigQueryOptions)))

;; Create connection
(def bq-conn (-> (BigQueryOptions/newBuilder)
                 ^BigQueryOptions$Builder
                 .build
                 .getService))

(def dataset-id (or (System/getenv "HUGSQL_BQ_TEST_DATASET_ID") "clojure_bq_test"))
(def fqn (format "%s.characters" dataset-id))
  
(def sql-file "/path/to/sql/file/containing/queries") ;; refer to resources/sq;/characters.sql

;; BigQuery uses mysql style quoting
(hugsql/def-db-fns sql-file {:quoting :mysql
                             :adapter (hugsql-bq/hugsql-adapter-bq)})

;; Fire the queries
(create-characters-table bq-conn {:fqn fqn})                   
(insert-character bq-conn {:name "Baby Groot"
                           :email nil
                           :age 7
                           :weapons ["Axe" "Wood"]
                           :details {:teams ["Guardians of the Galaxy" "Avengers"]
                                     :friendly true
                                     :num_movies 4}
                           :dob date
                           :superhero false
                           :weight 56.68
                           :code (byte-array code)
                           :net_worth (bigdec 10000)
                           :updated_at now})
```
### Running Test
```
GOOGLE_CLOUD_PROJECT=<your-gcp-project-name> lein test
```

### Parameters
You may pass `:field-name-transform-fn` to the hugsql adapter if you want to apply a fn on field names. By default, 
field names will be transformed as keyword.

### Supported Types

This adapter supports all the primitive BQ types. Following is the mapping of Clojure & BQ types:

| BigQuery  | Clojure                         |
|-----------|---------------------------------|
| TIMESTAMP | java.time.Instant               |
| DATE      | java.time.LocalDate             |
| TIME      | java.time.LocalTime             |
| DATETIME  | java.time.LocalDateTime         |
| BOOLEAN   | java.lang.Boolean               |
| FLOAT64   | java.lang.Double                |
| INT64     | java.lang.Long                  |
| NUMERIC   | java.math.BigDecimal            |
| STRING    | java.lang.String                |
| BYTES     | byte[]                          |
| ARRAY     | clojure.lang.PersistentList     |
| RECORD    | clojure.lang.PersistentArrayMap |
| NULL      | nil                             |

Few things to note:
- hugsql-bq uses `java-time` as its standard time library.
- `:keywords` will be coerced to STRING type.

### Unsupported Types

Currently `GEOGRAPHY` is not supported

Check `hugsql-bq-test` namespace for a detailed example of how to use this adapter.
