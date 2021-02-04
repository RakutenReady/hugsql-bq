(defproject hugsql-bq "0.2.4-SNAPSHOT"
  :description "HugSQL adapter for executing queries on BigQuery"
  :url "http://github.com/RakutenReady/hugsql-bq"
  :dependencies [[org.clojure/clojure "1.10.0"]

                 ;; Google BigQuery
                 [com.google.cloud/google-cloud-bigquery "1.124.2"]

                 ;; SQL
                 [com.layerware/hugsql "0.4.9"]

                 ;; Time
                 [clojure.java-time "0.3.2"]]
  :plugins [[lein-pprint "1.2.0"]
            [lein-cljfmt "0.6.3"]
            [com.gfredericks/lein-how-to-ns "0.2.2"]
            [lein-project-version "0.1.0"]
            [test2junit "1.4.2"]]
  :how-to-ns {:require-docstring?      false
              :sort-clauses?           true
              :allow-refer-all?        false
              :allow-extra-clauses?    false
              :align-clauses?          false
              :import-square-brackets? false}
  :deploy-repositories [["releases" {:url "https://maven.pkg.github.com/RakutenReady/hugsql-bq"
                                     :username :env/github_actor
                                     :password :env/github_token
                                     :sign-releases false}]]
  :aliases {"fix" ["do" ["cljfmt" "fix"] ["how-to-ns" "fix"]]}
  :repl-options {:init-ns hugsql.adapter.hugsql-bq}
  :main hugsql.adapter.hugsql-bq)
