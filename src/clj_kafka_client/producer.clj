(ns clj-kafka-client.producer
  (:refer-clojure :exclude [flush])
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer RecordMetadata)
           (java.util.concurrent Future TimeUnit TimeoutException)
           (org.apache.kafka.common.serialization Serializer StringSerializer
                                                  ByteArraySerializer LongSerializer
                                                  IntegerSerializer)
           (java.util Map)
           (org.apache.kafka.common PartitionInfo Node)))

(defn string-serializer [] (StringSerializer.))
(defn long-serializer [] (LongSerializer.))
(defn integer-serializer [] (IntegerSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  ([^Map configs] (producer configs (string-serializer) (string-serializer)))
  ([^Map configs ^Serializer key-serializer ^Serializer value-serializer]
   {:pre [(contains? configs "bootstrap.servers")]}
   (KafkaProducer. configs key-serializer value-serializer)))

(defn record
  ([topic value]
   (ProducerRecord. topic value))
  ([topic key value]
   (ProducerRecord. topic key value))
  ([topic partition key value]
   (ProducerRecord. topic partition key value)))

(defn- record-meta->map [^RecordMetadata meta]
  (when meta
    {:topic (.topic meta) :partition (.partition meta) :offset (.offset meta)}))

(defn ^Future send-record
  ([^KafkaProducer producer ^ProducerRecord record]
   (send-record producer record nil))
  ([^KafkaProducer producer ^ProducerRecord record call-back]
   (let [^Future fu (.send producer record call-back)]
     (reify
       Future
       (get [_] (-> (.get fu) (record-meta->map)))
       (get [_ timeout unit] (-> (.get fu timeout unit) (record-meta->map)))
       (isCancelled [_] (.isCancelled fu))
       (isDone [_] (.isDone fu))
       (cancel [_ interrupt?] (.cancel fu interrupt?))
       clojure.lang.IDeref
       (deref [_] (-> (.get fu) (record-meta->map)))
       clojure.lang.IBlockingDeref
       (deref [_ timeout-ms timeout-val]
         (try
           (-> (.get fu timeout-ms TimeUnit/MILLISECONDS) (record-meta->map))
           (catch TimeoutException ex
             timeout-val)))
       clojure.lang.IPending
       (isRealized [this] (.isDone fu))))))

(defn flush [^KafkaProducer producer]
  (.flush producer))

(defn- node->map [^Node node]
  {:id (.id node)
   :id-string (.idString node)
   :host (.host node)
   :port (.port node)})

(defn- partition-info->map [^PartitionInfo info]
  {:topic            (.topic info)
   :partition        (.partition info)
   :leader           (node->map (.leader info))
   :replicas         (mapv #(node->map %) (.replicas info))
   :in-sync-replicas (mapv #(node->map %) (.inSyncReplicas info))})

(defn partitions-for [^KafkaProducer producer ^String topic]
  (->> (.partitionsFor producer topic)
       (mapv #(partition-info->map %))))

(defn close
  ([^KafkaProducer producer]
   (.close producer Long/MAX_VALUE TimeUnit/MILLISECONDS))
  ([^KafkaProducer producer timeout time-unit]
   (.close producer timeout time-unit)))
