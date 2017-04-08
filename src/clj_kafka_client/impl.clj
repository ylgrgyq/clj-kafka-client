(ns clj-kafka-client.impl
  (:import (org.apache.kafka.common Node PartitionInfo TopicPartition)
           (org.apache.kafka.clients.consumer ConsumerRecords ConsumerRecord)))

(defprotocol ToMap
  (->map [this]))

(extend-protocol ToMap
  Node
  (->map [node]
    {:id (.id node)
     :id-string (.idString node)
     :host (.host node)
     :port (.port node)})

  PartitionInfo
  (->map [info]
    {:topic            (.topic info)
     :partition        (.partition info)
     :leader           (->map ^Node (.leader info))
     :replicas         (mapv #(->map %) (.replicas info))
     :in-sync-replicas (mapv #(->map %) (.inSyncReplicas info))})

  TopicPartition
  (->map [tp]
    {:topic (.topic tp)
     :partition (.partition tp)})

  ConsumerRecord
  (->map [record]
    {:topic (.topic record)
     :partition (.partition record)
     :offset (.offset record)
     :key (.key record)
     :value (.value record)})
  )
