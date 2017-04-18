(ns clj-kafka-client.admin
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata)
           (org.apache.kafka.common TopicPartition PartitionInfo)))

(defn get-partitions-for-topic [^KafkaConsumer consumer topic]
  (map #(.partition ^PartitionInfo %) (.partitionsFor consumer topic)))

(defn get-committed-offset-for-partition [^KafkaConsumer consumer topic partition]
  (if-let [^OffsetAndMetadata o (.committed consumer (TopicPartition. topic partition))]
    (.offset o)
    0))

(defn get-log-end-offset-for-partition [^KafkaConsumer consumer topic partition]
  (let [topic-partition (TopicPartition. topic partition)]
    (try
      (.assign consumer [topic-partition])
      (.seekToEnd consumer (into-array ^TopicPartition [topic-partition]))
      (.position consumer topic-partition)
      (catch Exception ex
        (log/warn "Get log end offset failed" topic partition)
        0))))

(defn get-lags-for-all-partitions [^KafkaConsumer consumer topic]
  (when-let [partitions (not-empty (get-partitions-for-topic consumer topic))]
    (apply sorted-map
           (interleave partitions
                       (map #(let [committed-offset (get-committed-offset-for-partition consumer topic %)
                                   log-end-offset   (get-log-end-offset-for-partition consumer topic %)]
                               (- log-end-offset committed-offset)) partitions)))))
