(ns clj-kafka-client.producer-test
  (:refer-clojure :exclude [flush])
  (:require [clojure.test :refer :all]
            [clj-kafka-client.producer :refer :all]))

(deftest test-produce
  (let [dummy-topic "topic"
        producer (kafka-producer {"bootstrap.servers" "localhost:9092"})
        fu (send-record producer (record dummy-topic "Hello world"))]
    (is (not-empty @fu))
    (is (= dummy-topic (:topic @fu)))
    (is (and (>= (:partition @fu) 0) (>= (:offset @fu) 0)))
    (close producer)))
