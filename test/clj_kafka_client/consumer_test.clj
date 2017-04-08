(ns clj-kafka-client.consumer-test
  (:use [clj-kafka-client.impl :only (->map)])
  (:require [clojure.test :refer :all]
            [clj-kafka-client.producer :refer [record send-record kafka-producer]]
            [clj-kafka-client.consumer :refer :all]
            [clojure.data.json :as json])
  (:import (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.producer Callback)))

(deftest ^:integration test-basic-consumer
  (let [p            (kafka-producer {"bootstrap.servers" "localhost:9092"})
        c            (kafka-consumer {"bootstrap.servers" "localhost:9092"
                                      "group.id"          "tester"})
        msg-id-start 210
        msg-id-end   400]
    (doseq [msg-id (range msg-id-start msg-id-end)]
      (send-record p (record "test" (json/write-str {:dummy-key "dummy-content1" :msg-id msg-id}))))

    (subscribe c "test")
    (is (= (- msg-id-end msg-id-start)
           (loop [i 0 ret 0]
             (if (< i 10)
               (let [r (poll c 100)]
                 (Thread/sleep 200)
                 (recur (inc i) (+ ret (count r))))
               ret))))
    (is (= #{"test"} (subscription c)))
    (clj-kafka-client.producer/close p)
    (unsubscribe c)
    (close c)))

(deftest ^:integration test-high-level-consumer
  (let [p                 (kafka-producer {"bootstrap.servers" "localhost:9092"})
        consumed-msg-ids  (atom [])
        last-consume-time (atom (System/currentTimeMillis))
        msg-id-start      410
        msg-id-end        600
        c                 (create-high-level-consumer {"bootstrap.servers" "localhost:9092"
                                                       "group.id"          "tester"}
                                                      "test"
                                                      (fn [msg]
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
      (send-record p (record "test" (json/write-str {:dummy-key "dummy-content2" :msg-id msg-id}))))

    (loop []
      (when (and (<= (- (System/currentTimeMillis) @last-consume-time) 5000)
                 (not= (count @consumed-msg-ids) (- msg-id-end msg-id-start)))
        (.sleep TimeUnit/SECONDS 1)
        (recur)))
    (.get (close-high-level-consumer c))
    (clj-kafka-client.producer/close p)
    (is (= (count @consumed-msg-ids) (count (into #{} @consumed-msg-ids))))

    (is (= (count @consumed-msg-ids) (- msg-id-end msg-id-start)))))