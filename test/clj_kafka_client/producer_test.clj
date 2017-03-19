(ns clj-kafka-client.producer-test
  (:require [clojure.test :refer :all]
            [clj-kafka-client.producer :refer :all]))

(deftest test-produce
  (let [dummy-topic "topic"
        kafka-producer (producer {"bootstrap.servers" "localhost:9092"})
        fu (send-record kafka-producer (record dummy-topic "Hello world"))]
    (is (not-empty @fu))
    (is (= dummy-topic (:topic @fu)))
    (is (and (>= (:partition @fu) 0) (>= (:offset @fu) 0)))))
