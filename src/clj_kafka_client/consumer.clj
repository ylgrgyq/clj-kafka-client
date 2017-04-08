(ns clj-kafka-client.consumer
  (:use [clj-kafka-client.impl :only (->map)])
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata
                                              ConsumerRebalanceListener OffsetCommitCallback ConsumerRecord ConsumerRecords)
           (java.util.concurrent Future ThreadFactory ArrayBlockingQueue TimeUnit ThreadPoolExecutor ExecutorService
                                 Executors ThreadPoolExecutor$AbortPolicy ExecutorCompletionService RejectedExecutionException ConcurrentLinkedQueue)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringDeserializer Deserializer
                                                  ByteArrayDeserializer IntegerDeserializer
                                                  LongDeserializer)
           (java.util Map LinkedList Collections List)
           (org.apache.kafka.common.errors WakeupException)
           (org.apache.kafka.clients.consumer.internals NoOpConsumerRebalanceListener)))

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
  (mapv ->map (.assignment consumer)))

(defn subscription [^KafkaConsumer consumer]
  (.subscription consumer))

(defn assign [^KafkaConsumer consumer partitions]
  (.assign consumer partitions))

(defn subscribe
  ([^KafkaConsumer consumer topics]
    (subscribe consumer topics nil))
  ([^KafkaConsumer consumer topics listener]
   (let [^List topics (if (string? topics) [topics] topics)
         ^ConsumerRebalanceListener listener (or listener (NoOpConsumerRebalanceListener.))]
     (.subscribe consumer topics listener))))

(defn unsubscribe [^KafkaConsumer consumer]
  (.unsubscribe consumer))

(defn poll [^KafkaConsumer consumer timeout]
  (map ->map (.poll consumer timeout)))

(defn seek-to-beginning [^KafkaConsumer consumer topic-partitinos]
  (.seekToBeginning consumer
                    (into-array TopicPartition (map #(TopicPartition. (:topic %) (:partition %)) topic-partitinos))))

(defn close [^KafkaConsumer consumer]
  (.close consumer))

(defn wakeup [^KafkaConsumer consumer]
  (.wakeup consumer))

(defn- get-commitable-partition-offset-pairs [queue-name partition->offset]
  (reduce #(let [[k v] %2]
             (assoc % (TopicPartition. queue-name k)
                      (OffsetAndMetadata. v))) {} partition->offset))

(defn- commit-offset
  ([queue-name consumer partition->offset] (commit-offset queue-name consumer partition->offset :sync))
  ([queue-name ^KafkaConsumer consumer partition->offset mode]
   (when-let [offsets (not-empty @partition->offset)]
     (let [commitable-offsets (get-commitable-partition-offset-pairs queue-name offsets)]
       (if (= :sync mode)
         (do
           (.commitSync consumer commitable-offsets)
           (reset! partition->offset {}))
         (.commitAsync consumer
                       commitable-offsets
                       (reify OffsetCommitCallback
                         (onComplete [this parmap ex]
                           (if-not ex
                             (doseq [[^TopicPartition k ^OffsetAndMetadata v] parmap]
                               (swap! partition->offset
                                      (fn [snapshot]
                                        (if (= (get snapshot (.partition k)) (.offset v))
                                          (dissoc snapshot (.partition k))
                                          snapshot))))
                             (log/warn ex "Async commit offset failed" queue-name commitable-offsets parmap))))))))))

(defn- decode-msg [queue-name msg-decoder msg]
  (when msg
    (try
      (if msg-decoder
        (msg-decoder msg)
        msg)
      (catch Exception ex
        (log/error ex "Decode msg failed" queue-name msg)))))

(def ^:private consumer-counter (atom -1))

(defn- create-worker-pool [topic pool-size task-queue-size]
  (let [thread-id (atom -1)]
    (ThreadPoolExecutor. pool-size
                         pool-size
                         0
                         TimeUnit/MILLISECONDS
                         (ArrayBlockingQueue. task-queue-size)
                         (reify ThreadFactory
                           (^Thread newThread [_ ^Runnable r]
                             (doto (.newThread (Executors/defaultThreadFactory) r)
                               (.setName (format "%s-consumer-thread-pool-%d-%d"
                                                 topic
                                                 (swap! consumer-counter inc)
                                                 (swap! thread-id inc)))
                               (.setDaemon true))))
                         (ThreadPoolExecutor$AbortPolicy.))))

(defn- update-offset-progress [partition->offset partition offset]
  (swap! partition->offset
         (fn [snapshot]
           (if (< (get snapshot partition 0) offset)
             (assoc snapshot partition offset)
             snapshot))))

(defn- process-complete-msg [^ExecutorCompletionService completion-service partition->offset]
  (loop [ret (.poll completion-service)]
    ;; Drain completion-service and swallow any exception
    (when ret
      (try
        (let [[partition offset] (.get ret)]
          (update-offset-progress partition->offset partition (inc offset)))
        (catch Exception _))
      (recur (.poll completion-service)))))

(defn- pause-subscribed-partitions [^KafkaConsumer consumer queue-name]
  (try
    (let [partitions (into-array TopicPartition (.assignment consumer))]
      (.pause consumer partitions)
      partitions)
    (catch Exception ex
      (log/error ex "Pause consumer failed" queue-name))))

(defn- process-pending-msgs [^KafkaConsumer consumer ^LinkedList pending-msgs process-record-fn]
  (when-not (.isEmpty pending-msgs)
    (loop []
      (when-let [[msg partition offset] (.peek pending-msgs)]
        (when (try (process-record-fn msg partition offset)
                   true
                   (catch RejectedExecutionException _
                     false))
          (.pop pending-msgs)
          (recur))))
    (.isEmpty pending-msgs)))

(defn- resume-partitions [^KafkaConsumer consumer queue-name]
  (try
    (let [partitions (.assignment consumer)]
      (.resume consumer (into-array TopicPartition partitions)))
    (catch IllegalStateException ex
      (log/warn ex "Resume partitions failed" queue-name)
      ;; Try again
      (.resume consumer (into-array TopicPartition (.assignment consumer))))))

(defn- ^ConsumerRebalanceListener create-consumer-rebalance-listener [consumer queue-name partition->offset auto-commit? paused-partitions]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [_ partitions]
      (log/info "Partitions was assigned" (map #(vector (.topic ^TopicPartition %)
                                                        (.partition ^TopicPartition %))
                                               partitions))
      (->> (pause-subscribed-partitions consumer queue-name)
           (reset! paused-partitions)))
    (onPartitionsRevoked [this partitions]
      (try
        (log/info "Partitions was revoked" (map #(vector (.topic ^TopicPartition %)
                                                         (.partition ^TopicPartition %))
                                                partitions))
        (when-not auto-commit?
          (commit-offset queue-name consumer partition->offset))
        (catch Exception ex
          (log/warn ex "Commit offset failed" queue-name))))))

(defn- shutdown-worker-pool [^ExecutorService worker-pool worker-pool-graceful-shutdown-timeout-ms]
  (when worker-pool
    (.shutdown worker-pool)
    (try
      ; clean interrupt status so we can await worker pool terminate
      (Thread/interrupted)
      (.awaitTermination worker-pool
                         worker-pool-graceful-shutdown-timeout-ms
                         TimeUnit/MILLISECONDS)
      (catch InterruptedException _))
    (.shutdownNow worker-pool)))

(defn create-high-level-consumer [consumer-configs queue-name msg-handler-fn & opts]
  (let [stopped?                     (atom false)
        default-factory              (Executors/defaultThreadFactory)
        {:keys [interval reliable start-consumer-from-largest msg-decoder
                key-deserializer value-deserializer worker-pool-size worker-pool-task-queue-size
                worker-pool-graceful-shutdown-timeout-ms]
         :or   {worker-pool-size                         (.availableProcessors (Runtime/getRuntime))
                interval                                 100
                start-consumer-from-largest              false
                worker-pool-graceful-shutdown-timeout-ms 3000
                worker-pool-task-queue-size              1000
                key-deserializer                         (string-deserializer)
                value-deserializer                       (string-deserializer)}} opts
        auto-commit?                 (= (get consumer-configs "enable.auto.commit" "true") "true")
        paused-partitions            (atom nil)
        ^ExecutorService worker-pool (create-worker-pool queue-name worker-pool-size worker-pool-task-queue-size)
        completion-service           (ExecutorCompletionService. worker-pool)
        consumer                     (consumer consumer-configs key-deserializer value-deserializer)
        partition->offset            (atom {})
        pending-msgs                 (ConcurrentLinkedQueue.)
        msg-processor-fn             (fn [msg partition offset]
                                       (.submit completion-service
                                                (fn []
                                                  (msg-handler-fn msg)
                                                  [partition offset])))
        fu                           (future-call
                                       (fn []
                                         (.subscribe consumer
                                                     (Collections/singletonList queue-name)
                                                     (create-consumer-rebalance-listener consumer queue-name partition->offset auto-commit? paused-partitions))
                                         (when start-consumer-from-largest
                                           (seek-to-beginning consumer []))
                                         (try
                                           (while (not @stopped?)
                                             (try
                                               (let [^ConsumerRecords records (.poll consumer interval)]
                                                 (doseq [^ConsumerRecord record records]
                                                   (let [partition (.partition record)
                                                         offset    (.offset record)]
                                                     (if-let [msg (not-empty (decode-msg queue-name msg-decoder (.value record)))]
                                                       (try
                                                         (if @paused-partitions
                                                           (.add pending-msgs [msg partition offset])
                                                           (msg-processor-fn msg partition offset))
                                                         (catch RejectedExecutionException _
                                                           (.add pending-msgs [msg partition offset])
                                                           (->> (pause-subscribed-partitions consumer queue-name)
                                                                (reset! paused-partitions))))
                                                       (update-offset-progress partition->offset partition (inc offset))))))
                                               (when (process-pending-msgs consumer pending-msgs msg-processor-fn)
                                                 (resume-partitions consumer queue-name)
                                                 (reset! paused-partitions nil))
                                               (catch WakeupException _)
                                               (catch InterruptedException _)
                                               (catch Exception ex
                                                 (log/errorf ex "%s queue got unexpected error" queue-name))
                                               (finally
                                                 (try
                                                   (process-complete-msg completion-service partition->offset)
                                                   (when-not auto-commit?
                                                     (commit-offset queue-name consumer partition->offset))
                                                   (catch Exception ex
                                                     (log/errorf ex "Commit offset for queue %s failed" queue-name))))))
                                           (finally
                                             (try
                                               (shutdown-worker-pool worker-pool worker-pool-graceful-shutdown-timeout-ms)
                                               (process-complete-msg completion-service partition->offset)
                                               (when-not auto-commit?
                                                 ;; Try to commit offset again
                                                 (commit-offset queue-name consumer partition->offset))
                                               (close consumer)
                                               (catch Exception ex
                                                 (log/errorf "Close consumer for queue %s failed" queue-name)))))))]
    {:consumer     consumer :future fu :paused? paused-partitions :stopped? stopped?
     :pending-msgs pending-msgs :partition->offset partition->offset}))

(defn stop-high-level-consumer [high-level-consumer]
  (when-let [{:keys [consumer stopped? ^Future future]} high-level-consumer]
    (reset! stopped? true)
    (wakeup consumer)
    (when (and future (not (.isDone future)))
      (.cancel future true))
    (reify java.util.concurrent.Future
      (get [_] (try
                 (some-> future (deref))
                 (catch Exception _))
        "OK")
      (get [_ timeout unit]
        (when future
          (let [timeout-ms (.toMillis unit timeout)]
            (deref future timeout-ms nil)
            "OK")))
      (isCancelled [_] false)
      (isDone [_] (.isDone future))
      (cancel [_ interrupt?] (.cancel future interrupt?)))))