(ns clj-kafka-client.consumer-group
  (:require [clojure.tools.logging :as log]
            [clj-kafka-client.consumer :refer :all])
  (:import (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.clients.consumer OffsetAndMetadata KafkaConsumer OffsetCommitCallback ConsumerRebalanceListener ConsumerRecords ConsumerRecord)
           (java.util.concurrent ThreadPoolExecutor$AbortPolicy Executors ThreadFactory ArrayBlockingQueue TimeUnit ThreadPoolExecutor ExecutorCompletionService ExecutorService RejectedExecutionException Future ConcurrentLinkedQueue)
           (java.util LinkedList Collections)
           (org.apache.kafka.common.serialization StringDeserializer)
           (org.apache.kafka.common.errors WakeupException)))

(defn- process-msg [processor-fn msg abandoned-msg-handler process-msg-max-retry-times]
  (loop [retry 0]
    (let [succ? (try
                  (processor-fn msg)
                  true
                  (catch InterruptedException ie
                    ;;break out,but don't ack.
                    (throw ie))
                  (catch Exception e
                    (log/error e "Processing message " msg " faild.Retry " retry " times")
                    false))]
      (when-not succ?
        (if (< retry process-msg-max-retry-times)
          (do
            ;;sleep and retry it
            (Thread/sleep 500)
            (recur (inc retry)))
          (abandoned-msg-handler msg))))))

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

(defn start-consume-queue [consumer-configs queue-name handler opts]
  (let [stopped?                     (atom false)
        default-factory              (Executors/defaultThreadFactory)
        {:keys [interval reliable abandoned-msg-handler start-consumer-from-largest msg-decoder
                key-deserializer value-deserializer worker-pool-size worker-pool-task-queue-size
                worker-pool-graceful-shutdown-timeout-ms process-msg-max-retry-times]
         :or   {worker-pool-size                         (.availableProcessors (Runtime/getRuntime))
                abandoned-msg-handler                    (constantly nil)
                interval                                 100
                process-msg-max-retry-times              3
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
        process-record-fn            (fn [msg partition offset]
                                       (.submit completion-service
                                                (fn []
                                                  (process-msg handler msg abandoned-msg-handler process-msg-max-retry-times)
                                                  [partition offset])))
        fu                           (future-call
                                       (fn []
                                         (.subscribe consumer
                                                     (Collections/singletonList queue-name)
                                                     (create-consumer-rebalance-listener consumer queue-name partition->offset auto-commit? paused-partitions))
                                         (when start-consumer-from-largest
                                           (seek-to-beginning consumer []))
                                         (let [abandoned-msg-handler (try abandoned-msg-handler
                                                                          (catch Exception _))]
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
                                                             (process-record-fn msg partition offset))
                                                           (catch RejectedExecutionException _
                                                             (.add pending-msgs [msg partition offset])
                                                             (->> (pause-subscribed-partitions consumer queue-name)
                                                                  (reset! paused-partitions))))
                                                         (update-offset-progress partition->offset partition (inc offset))))))
                                                 (when (process-pending-msgs consumer pending-msgs process-record-fn)
                                                   (resume-partitions consumer queue-name)
                                                   (reset! paused-partitions nil))
                                                 (catch WakeupException _)
                                                 (catch InterruptedException _)
                                                 (catch Exception ex
                                                   (log/error ex "Queue error" queue-name))
                                                 (finally
                                                   (try
                                                     (process-complete-msg completion-service partition->offset)
                                                     (when-not auto-commit?
                                                       (commit-offset queue-name consumer partition->offset))
                                                     (catch Exception ex
                                                       (log/error ex "Commit offset failed" queue-name))))))
                                             (finally
                                               (try
                                                 (shutdown-worker-pool worker-pool worker-pool-graceful-shutdown-timeout-ms)
                                                 (process-complete-msg completion-service partition->offset)
                                                 (when-not auto-commit?
                                                   ;; Try to commit offset again
                                                   (commit-offset queue-name consumer partition->offset))
                                                 (.close ^KafkaConsumer consumer)
                                                 (log/infof "Subscribe %s queue thread exited" queue-name)
                                                 (catch Exception ex
                                                   (log/error "Close consumer failed" queue-name))))))))]
    {:consumer     consumer :future fu :paused? paused-partitions :stopped? stopped?
     :pending-msgs pending-msgs :partition->offset partition->offset}))

(defn stop-consume-queue [subscribed-consumer]
  (when-not (empty? subscribed-consumer)
    (let [{:keys [consumer stopped? ^Future future]} subscribed-consumer]
      (reset! stopped? true)
      (.wakeup ^KafkaConsumer consumer)
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
        (cancel [_ interrupt?] (.cancel future interrupt?))))))