(ns clj-kafka-client.consumer
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata
                                              ConsumerRebalanceListener OffsetCommitCallback ConsumerRecord ConsumerRecords)
           (java.util.concurrent Future ThreadFactory ArrayBlockingQueue TimeUnit ThreadPoolExecutor ExecutorService
                                 Executors ThreadPoolExecutor$AbortPolicy ExecutorCompletionService RejectedExecutionException)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringDeserializer Deserializer
                                                  ByteArrayDeserializer IntegerDeserializer
                                                  LongDeserializer)
           (java.util Map LinkedList Collections)
           (org.apache.kafka.common.errors WakeupException)))

(defn string-deserializer [] (StringDeserializer.))
(defn long-deserializer [] (LongDeserializer.))
(defn integer-deserializer [] (IntegerDeserializer.))
(defn byte-array-deserializer [] (ByteArrayDeserializer.))

(defn ^KafkaConsumer consumer
  ([^Map configs] (consumer configs (string-deserializer) (string-deserializer)))
  ([^Map configs ^Deserializer key-deserializer ^Deserializer value-deserializer]
   {:pre [(contains? configs "bootstrap.servers")
          (contains? configs "group.id")]}
   (KafkaConsumer. ^Map configs key-deserializer value-deserializer)))

(defn assignment [^KafkaConsumer consumer]
  (into #{} (.assignment consumer)))

(defn subscription [^KafkaConsumer consumer]
  (into #{} (.subscription consumer)))

(defn assign [^KafkaConsumer consumer partitions]
  (.assign consumer partitions))

(defn unsubscribe [^KafkaConsumer consumer]
  (.unsubscribe consumer))

(defn seek-to-beginning [^KafkaConsumer consumer topic-partitinos]
  (.seekToBeginning consumer
                    (into-array TopicPartition (map #(TopicPartition. (:topic %) (:partition %)) topic-partitinos))))

(defn close [^KafkaConsumer consumer]
  (.close consumer))

(defn wakeup [^KafkaConsumer consumer]
  (.wakeup consumer))


