(ns hugsql.adapter.hugsql-bq
  (:require
   [clojure.reflect :as cr]
   [clojure.walk :as walk]
   [hugsql.adapter :as adapter]
   [java-time :as time])
  (:import
   (com.google.cloud RetryOption)
   (com.google.cloud.bigquery StandardSQLTypeName LegacySQLTypeName QueryParameterValue QueryJobConfiguration$Builder JobInfo BigQuery$JobOption JobId Job TableResult BigQuery$QueryResultsOption QueryJobConfiguration FieldValue Field Field$Mode FieldList FieldValueList)
   (java.time Instant)
   (java.time.temporal ChronoUnit)
   (java.util UUID))
  (:gen-class))

(def ^:private type-map {"java.lang.Long" StandardSQLTypeName/INT64
                         "java.lang.Integer" StandardSQLTypeName/INT64
                         "java.lang.String" StandardSQLTypeName/STRING
                         "clojure.lang.Keyword" StandardSQLTypeName/STRING
                         "java.lang.Boolean" StandardSQLTypeName/BOOL
                         "java.lang.Float" StandardSQLTypeName/FLOAT64
                         "java.lang.Double" StandardSQLTypeName/FLOAT64
                         "java.math.BigDecimal" StandardSQLTypeName/NUMERIC
                         "java.math.BigInteger" StandardSQLTypeName/NUMERIC
                         "clojure.lang.BigInt" StandardSQLTypeName/NUMERIC
                         "byte[]" StandardSQLTypeName/BYTES
                         "java.time.LocalTime" StandardSQLTypeName/TIME
                         "java.time.LocalDate" StandardSQLTypeName/DATE
                         "java.time.LocalDateTime" StandardSQLTypeName/DATETIME
                         "java.time.Instant" StandardSQLTypeName/TIMESTAMP
                         "clojure.lang.PersistentVector" StandardSQLTypeName/ARRAY
                         "clojure.lang.PersistentList" StandardSQLTypeName/ARRAY
                         "clojure.lang.LazySeq" StandardSQLTypeName/ARRAY
                         "clojure.lang.PersistentArrayMap" StandardSQLTypeName/STRUCT})

(defn- get-type-name
  [item]
  (-> item
      type
      cr/typename))

(defn- get-component-type
  "Returns the StandardSQLTypeName for the Clojure type inside the seq."
  [seq]
  (when (empty? seq)
    (throw (ex-info "Found empty seq. Use nil instead." {:data seq})))
  ;; Ensure the types inside the context are same
  (let [type-name (get-type-name (first seq))]
    (when (nil? (get type-map type-name))
      (throw (ex-info (format "Data type not supported : %s" type-name) {})))
    (when-not (every? #(= type-name (get-type-name %)) seq)
      (throw (ex-info "Items in the seq must be of the same type" {:data seq}))))
  (->> seq
       first
       get-type-name
       (get type-map)))

(defn- coerce-item
  "Casts certain Clojure types into supported Java types."
  [item]
  (let [type-name (get-type-name item)]
    (case type-name
      "clojure.lang.Keyword" (name item)
      "java.math.BigInteger" (bigdec item)
      "clojure.lang.BigInt" (bigdec item)
      "java.time.LocalTime" (str item)
      "java.time.LocalDate" (str item)
      "java.time.LocalDateTime" (str item)
      ;; BQ supports microsecond precision but instant does not have toEpochMicros()
      "java.time.Instant" (.between ChronoUnit/MICROS Instant/EPOCH item)
      item)))

(defn- clj-type->bq-type
  "Returns a BigQuery casting function for the given Clojure type."
  [item]
  (let [type-name (get-type-name item)]
    (when (nil? (get type-map type-name))
      (throw (ex-info (format "Data type not supported : %s" type-name) {})))
    (let [process-seq-fn (fn [item] (-> (QueryParameterValue/newBuilder)
                                        (.setArrayType (get-component-type item))
                                        (.setArrayValues (map clj-type->bq-type item))
                                        (.setType StandardSQLTypeName/ARRAY)
                                        .build))
          process-map-fn (fn [item] (->> item
                                         (walk/walk (fn [[k v]] [(name k) (clj-type->bq-type v)]) identity)
                                         QueryParameterValue/struct))]
      (case type-name
        "clojure.lang.PersistentArrayMap" (process-map-fn item)
        "clojure.lang.PersistentVector" (process-seq-fn item)
        "clojure.lang.PersistentList" (process-seq-fn item)
        "clojure.lang.LazySeq" (process-seq-fn item)
        (QueryParameterValue/of (coerce-item item) ^StandardSQLTypeName (get type-map type-name))))))

(defn- ^QueryJobConfiguration$Builder add-param
  "Adds a single positional parameter to QueryJobConfiguration$Builder."
  [^QueryJobConfiguration$Builder builder item]
  (->> item
       clj-type->bq-type
       (.addPositionalParameter builder)))

(defn nth-?
  "Find the index of the nth positional param in the query."
  [query n]
  (->> query
       (map vector (range))
       (filter (fn [[_i char]] (= char \?)))
       (#(nth % n))
       first))

(defn- replace-nth-with-null
  "Replace the nth positional param with a NULL."
  [query n]
  (let [index (nth-? query n)]
    (if index
      (apply str (concat
                  (subs query 0 index)
                  "NULL"
                  (subs query (inc index) (count query))))
      (throw (ex-info (format "No positional argument found at position %d" n) {:query query})))))

(defn- insert-nulls
  "For every Clojure nil, replace the corresponding positional param with a NULL."
  [sqlvec]
  (let [query (first sqlvec)
        all-params (rest sqlvec)]
    (loop [n 0
           query query
           params all-params]
      (if (< n (count params))
        (let [param (nth params n)]
          (if (nil? param)
            (recur n
                   (replace-nth-with-null query n)
                   (concat (take n params) (drop (inc n) params)))
            (recur (inc n)
                   query
                   params)))
        [query params]))))

(defn ^QueryJobConfiguration sqlvec->bq
  "Converts a sqlvec to a QueryJobConfiguration that can be sent to BigQuery.
  Uses positional parameters API of the BigQuery client.

  See: https://cloud.google.com/bigquery/docs/parameterized-queries#java"
  [sqlvec]
  (let [[query params] (insert-nulls sqlvec)
        ^QueryJobConfiguration$Builder builder (QueryJobConfiguration/newBuilder query)]
    (->> params
         (reduce add-param builder)
         .build)))

(defn- job-id
  "Returns a UUID string prefixed with fn-name if specified, UUID string otherwise."
  [{prefix :fn-name}]
  (JobId/of
   (if prefix
     (format "%s-%s" prefix (str (UUID/randomUUID)))
     (str (UUID/randomUUID)))))

(defn- ^Job await-completion
  "Waits for the job to finish then returns the job if successful."
  [^Job job]
  (let [status (-> job
                   (.waitFor (into-array RetryOption []))
                   .getStatus)]
    (if-let [err (.getError status)]
      (throw (ex-info
              (format "Failed to execute query: %s" (.getReason err)) {:err err}))
      job)))

(defn- ^TableResult get-table-result
  [^Job job]
  (.getQueryResults job (into-array BigQuery$QueryResultsOption [])))

(declare coerce-row)

(defn- bq-type->clj-type
  [^Field schema ^FieldValue val opts]
  (if (.isNull val)
    nil
    (condp = (.getType schema)
      StandardSQLTypeName/TIMESTAMP (time/instant (.getValue val))
      LegacySQLTypeName/BOOLEAN (.getBooleanValue val)
      LegacySQLTypeName/INTEGER (.getLongValue val)
      LegacySQLTypeName/BYTES (.getBytesValue val)
      LegacySQLTypeName/STRING (.getStringValue val)
      LegacySQLTypeName/FLOAT (.getDoubleValue val)
      LegacySQLTypeName/NUMERIC (.getNumericValue val)
      ;; the Instant class doesn't have an ofEpochMicros(long) method,
      ;; it does has an overload of ofEpochSecond(long, long) that
      ;; accepts a nanosecond offset along with the epoch seconds.
      LegacySQLTypeName/TIMESTAMP (let [microseconds (.getTimestampValue val)
                                        seconds (/ microseconds 1000000)
                                        nanoseconds (* (mod microseconds 1000000) 1000)]
                                    (Instant/ofEpochSecond seconds nanoseconds))
      LegacySQLTypeName/TIME (time/local-time (.getValue val))
      LegacySQLTypeName/DATE (time/local-date (.getValue val))
      LegacySQLTypeName/DATETIME (time/local-date-time (.getValue val))
      LegacySQLTypeName/RECORD (coerce-row (.getSubFields schema) (.getRecordValue val) opts)
      (.getValue val))))

(defn- coerce-col-val
  [^Field schema ^FieldValue val opts]
  (if (= (.getMode schema) Field$Mode/REPEATED)
    (map #(bq-type->clj-type schema % opts) (.getRepeatedValue val))
    (bq-type->clj-type schema val opts)))

(defn- coerce-row
  [^FieldList schema
   ^FieldValueList row
   {:keys [field-name-transform-fn] :or {field-name-transform-fn keyword} :as opts}]
  {:pre [(fn? field-name-transform-fn)]}
  (reduce
   (fn [acc ^Field f]
     (let [fname (.getName f)
           col-val (.get row (.getIndex schema fname))]
       (assoc acc (field-name-transform-fn fname) (coerce-col-val f col-val opts))))
   {}
   schema))

(defn result->clj
  "Given a BigQuery TableResult, return a lazy seq of results as idiomatic
   Clojure map forms. (Result sets may be huge; returned sequence is lazy.)"
  [^TableResult res opts]
  ;; See: https://cloud.google.com/bigquery/docs/schemas#column_names
  ;; Note: Very important to keep this **lazy** all the way through
  (if-let [schema (.getSchema res)]
    (let [schema-fields (.getFields schema)
          items (seq (.iterateAll res))]
      (map #(coerce-row schema-fields % opts) items))
    (throw (RuntimeException. "No schema"))))

(deftype HugsqlAdapterBigQuery [opts]
  adapter/HugsqlAdapter

  (execute [this db sqlvec options]
    (let [job-conf (sqlvec->bq sqlvec)
          job-id (job-id options)
          job-info (-> job-conf
                       JobInfo/newBuilder
                       (.setJobId job-id)
                       .build)]
      (-> db
          (.create job-info (into-array BigQuery$JobOption []))
          await-completion)))

  (query [this db sqlvec options]
    (adapter/execute this db sqlvec options))

  (result-one [this result options]
    (-> result
        get-table-result
        (result->clj opts)
        first))

  (result-many [this result options]
    (-> result
        get-table-result
        (result->clj opts)))

  (result-affected [this result options]
    (-> result
        ^TableResult get-table-result
        .getTotalRows))

  (result-raw [this result options]
    (get-table-result result))

  (on-exception [this exception]
    (throw exception)))

(defn hugsql-adapter-bq
  "The options map support:
  `:field-name-transform-fn` - optional - `fn that will be applied on each field name`, default keyword fn is going to be applied."
  ([]
   (hugsql-adapter-bq {}))
  ([opts]
   (->HugsqlAdapterBigQuery opts)))
