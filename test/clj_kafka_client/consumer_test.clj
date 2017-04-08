(ns clj-kafka-client.consumer-test
  (:require [clojure.test :refer :all]
            [clj-kafka-client.producer :refer [record send-record]]
            [clj-kafka-client.consumer :refer :all]
            [clojure.data.json :as json])
  (:import (java.util.concurrent TimeUnit)))


(deftest ^:integration test-plain-produce-consume
  (let [p                 (producer {"bootstrap.servers" "localhost:9092"})
        consumed-msg-ids  (atom [])
        last-consume-time (atom (System/currentTimeMillis))
        msg-id-start      10
        msg-id-end        200
        c                 (create-high-level-consumer {"bootstrap.servers" "localhost:9092"
                                                       "group.id"          "tester"
                                                       "fetch.wait.max.ms" "10"}
                                                      "test"
                                                      (fn [msg]
                                                        (println "sss" msg)
                                                        (let [msg (json/read-str msg :key-fn keyword)]
                                                          (println (System/currentTimeMillis) "got msg" msg)
                                                          (Thread/sleep 100)
                                                          (swap! consumed-msg-ids conj (:msg-id msg))
                                                          (swap! last-consume-time (fn [old]
                                                                                     (let [now (System/currentTimeMillis)]
                                                                                       (if (> now old)
                                                                                         now
                                                                                         old))))))
                                                      :worker-pool-size 1
                                                      :worker-pool-task-queue-size 1)]
    (doseq [msg-id (range msg-id-start msg-id-end)]
      (send-record p (record "test" (json/write-str {:dummy-key "dummy-content" :msg-id msg-id}))))

    (loop []
      (when (and (<= (- (System/currentTimeMillis) @last-consume-time) 5000)
                 (not= (count @consumed-msg-ids) (- msg-id-end msg-id-start)))
        ;(.sleep TimeUnit/SECONDS 1)
        (recur)))
    (.get (close-high-level-consumer c))
    (clj-kafka-client.producer/close p)
    (is (= (count @consumed-msg-ids) (count (into #{} @consumed-msg-ids))))

    (is (= (count @consumed-msg-ids) (- msg-id-end msg-id-start)))))