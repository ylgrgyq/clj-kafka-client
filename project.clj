(defproject clj-kafka-client "0.2.0-SNAPSHOT"
  :description "A clojure API wrapper for Kafka new client"
  :url "http://github.com/ylgrgyq/clj-kafka-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.6"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-clients "0.9.0.1"]]
  :plugins [[lein-codox "0.9.5"]]
  :codox {:output-path "target/codox"
          :source-uri "https://github.com/ylgrgyq/clj-kafka-client/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :deploy-repositories {"releases" :clojars}
  :global-vars {*warn-on-reflection* true
                *assert* false})