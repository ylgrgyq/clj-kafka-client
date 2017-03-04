(ns clj-kafka-client.producer
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer RecordMetadata)
           (java.util.concurrent Future)
           (org.apache.kafka.common.serialization Serializer StringSerializer
                                                  ByteArraySerializer LongSerializer
                                                  IntegerSerializer)
           (java.util Map)))

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
     (reify Future
       (get [_] (-> (.get fu) (record-meta->map)))
       (get [_ timeout unit] (-> (.get fu timeout unit) (record-meta->map)))
       (isCancelled [_] (.isCancelled fu))
       (isDone [_] (.isDone fu))
       (cancel [_ interrupt?] (.cancel fu interrupt?))))))
