(ns hugsql.adapter.hugsql-bq-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [hugsql.adapter.hugsql-bq :as hugsql-bq]
   [hugsql.core :as hugsql]
   [java-time :as time])
  (:import
   (com.google.cloud.bigquery BigQueryOptions$Builder BigQueryOptions)))

(def ^:private dataset-id (or (System/getenv "HUGSQL_BQ_TEST_DATASET_ID") "clojure_bq_test"))
(def ^:private users-sql-defs "../resources/sql/characters.sql")
(def ^:private string-transform-users-sql-defs "../resources/sql/string-transform-characters.sql")
(def ^:private bq-conn (-> (BigQueryOptions/newBuilder)
                           ^BigQueryOptions$Builder
                           .build
                           .getService))
(def ^:private date (time/local-date))
(def ^:private now (time/instant))
(def ^:private code [1 2 3])
(def ^:private fqn (format "%s.characters" dataset-id))
(def ^:private a-character {:name "Baby Groot"
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

(def ^:private a-character-string-key-case {"name" "Baby Groot"
                                            "email" nil
                                            "age" 7
                                            "weapons" ["Axe" "Wood"]
                                            "details" {"teams" ["Guardians of the Galaxy" "Avengers"]
                                                       "friendly" true
                                                       "num_movies" 4}
                                            "dob" date
                                            "superhero" false
                                            "weight" 56.68
                                            "code" (byte-array code)
                                            "net_worth" (bigdec 10000)
                                            "updated_at" now})

(def ^:private characters-tuple [["Captain Marvel"
                                  "captain.marvel@marvel.com"
                                  23
                                  ["Shield" "Thor's Hammer"]
                                  date
                                  true
                                  98.1
                                  (byte-array code)
                                  (bigdec 1001201293)
                                  now]
                                 ["Iron Man"
                                  nil
                                  51
                                  nil
                                  date
                                  true
                                  182.0
                                  (byte-array code)
                                  (bigdec 9001)
                                  now]])

(def ^:private first-expected-character-from-tuple {:name       "Captain Marvel"
                                                    :email      "captain.marvel@marvel.com"
                                                    :age        23
                                                    :weapons    ["Shield" "Thor's Hammer"]
                                                    :details    nil
                                                    :dob        date
                                                    :superhero  true
                                                    :weight     98.1
                                                    :code       (byte-array code)
                                                    :net_worth  (bigdec 1001201293)
                                                    :updated_at now})

(def ^:private second-expected-character-from-tuple {:name       "Iron Man"
                                                     :email      nil
                                                     :age        51
                                                     :weapons    []
                                                     :details    nil
                                                     :dob        date
                                                     :superhero  true
                                                     :weight     182.0
                                                     :code       (byte-array code)
                                                     :net_worth  (bigdec 9001)
                                                     :updated_at now})

(hugsql/def-db-fns users-sql-defs {:quoting :mysql
                                   :adapter (hugsql-bq/hugsql-adapter-bq)})

(hugsql/def-db-fns string-transform-users-sql-defs
                   {:quoting :mysql
                    :adapter (hugsql-bq/hugsql-adapter-bq {:field-name-transform-fn str})})

(deftest type-coercion-test
  (testing "Test hugsql-bq lifecyle"
    ;; Cleanup
    (drop-characters-table bq-conn {:fqn fqn})
    (is (= 0 (create-characters-table bq-conn {:fqn fqn})))
    (is (= 0 (insert-character bq-conn (assoc a-character :fqn fqn))))
    (is (= 0 (insert-characters bq-conn {:characters characters-tuple :fqn fqn} )))
    (let [query-res (first (select-character bq-conn {:fqn fqn
                                                      :name "Captain Marvel"}))]
      ;; We need to convert byte arrays back to compare them
      (is (= (update first-expected-character-from-tuple :code vec) (update query-res :code vec))))
    (let [query-res (first (select-character bq-conn {:fqn fqn
                                                      :name "Iron Man"}))]
      ;; We need to convert byte arrays back to compare them
      (is (= (update second-expected-character-from-tuple :code vec) (update query-res :code vec))))
    (let [query-res (first (select-character bq-conn {:fqn fqn
                                                      :name "Baby Groot"}))]
      ;; We need to convert byte arrays back to compare them
      (is (= (update a-character :code vec) (update query-res :code vec))))
    (let [query-res (first (string-transform-select-character bq-conn {:fqn fqn
                                                                       :name "Baby Groot"}))]

      (is (= (update a-character-string-key-case "code" vec) (update query-res "code" vec))))))
