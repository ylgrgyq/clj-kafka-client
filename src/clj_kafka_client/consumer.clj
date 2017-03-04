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

(defn- process-msg [worker msg abandoned-msg-handler process-msg-max-retry-times]
  (loop [retry 0]
    (let [succ? (try
                  (worker msg)
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
          (do
            (log/warn
              "Could not process message " msg ".Retried too many times.")
            (abandoned-msg-handler msg)))))))

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

(defn- ^ConsumerRebalanceListener create-consumer-rebalance-listener [consumer queue-name partition->offset auto-commit?]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [_ partitions]
      (log/info "Partitions was assigned" (map #(vector (.topic ^TopicPartition %)
                                                        (.partition ^TopicPartition %))
                                               partitions)))
    (onPartitionsRevoked [this partitions]
      (try
        (log/info "Partitions was revoked" (map #(vector (.topic ^TopicPartition %)
                                                         (.partition ^TopicPartition %))
                                                partitions))
        (when-not auto-commit?
          (commit-offset queue-name consumer partition->offset))
        (catch Exception ex
          (log/warn ex "Commit offset failed" queue-name))))))

(defn- update-offset-progress [partition->offset partition offset]
  (swap! partition->offset
         (fn [snapshot]
           (if (< (get snapshot partition 0) offset)
             (assoc snapshot partition offset)
             snapshot))))

(defn- process-complete-msg [^ExecutorCompletionService completion-service partition->offset]
  (when completion-service
    (loop [ret (.poll completion-service)]
      ;; Drain completion-service and swallow any exception
      (when ret
        (try
          (let [[partition offset] (.get ret)]
            (update-offset-progress partition->offset partition (inc offset)))
          (catch Exception _))
        (recur (.poll completion-service))))))

(defn- pause-subscribed-partitions [^KafkaConsumer consumer queue-name]
  (try
    (let [partitions (into-array TopicPartition (.assignment consumer))]
      (.pause consumer partitions)
      partitions)
    (catch Exception ex
      (log/error ex "Pause consumer failed" queue-name))))

(defn- reprocess-pending-msgs [^KafkaConsumer consumer ^LinkedList pending-msgs process-record-fn]
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

(defn subscribe [consumer-configs queue-name handler opts]
  (let [{:keys [consumer-count] :or {consumer-count 1}} opts
        stopped?  (atom false)
        thread-id (atom -1)
        default-factory (Executors/defaultThreadFactory)
        consumers (-> (fn [_]
                        (let [{:keys [interval reliable abandoned-msg-handler start-consumer-from-largest msg-decoder
                                      key-deserializer value-deserializer worker-pool-size
                                      worker-pool-task-queue-size
                                      worker-pool-graceful-shutdown-timeout-ms process-msg-max-retry-times]
                               :or   {abandoned-msg-handler                    (constantly nil)
                                      interval                                 100
                                      process-msg-max-retry-times              3
                                      start-consumer-from-largest              false
                                      worker-pool-graceful-shutdown-timeout-ms 3000
                                      worker-pool-task-queue-size              1000
                                      key-deserializer                         (StringDeserializer.)
                                      value-deserializer                       (StringDeserializer.)}} opts
                              auto-commit?                 (= (get consumer-configs "enable.auto.commit" "true") "true")
                              paused-partitions            (atom nil)
                              ^ExecutorService worker-pool (when worker-pool-size
                                                             (create-worker-pool queue-name
                                                                                 worker-pool-size
                                                                                 worker-pool-task-queue-size))
                              completion-service           (some-> worker-pool (ExecutorCompletionService.))
                              consumer                     (consumer consumer-configs key-deserializer value-deserializer)
                              partition->offset            (atom {})
                              pending-msgs                 (LinkedList.)
                              process-record-fn (if completion-service
                                                  (fn [msg partition offset]
                                                    (.submit completion-service
                                                             (fn []
                                                               (process-msg handler msg abandoned-msg-handler process-msg-max-retry-times)
                                                               [partition offset])))
                                                  (fn [msg partition offset]
                                                    (process-msg handler msg abandoned-msg-handler process-msg-max-retry-times)
                                                    (update-offset-progress partition->offset partition (inc offset))))
                              fu (future-call
                                   (fn []
                                     (.subscribe consumer
                                                 (Collections/singletonList queue-name)
                                                 (create-consumer-rebalance-listener consumer queue-name partition->offset auto-commit?))
                                     (when start-consumer-from-largest
                                       (.seekToEnd consumer (into-array TopicPartition [])))
                                     (let [last-log-time         (atom 0)
                                           abandoned-msg-handler (try abandoned-msg-handler
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
                                             (when (reprocess-pending-msgs consumer pending-msgs process-record-fn)
                                               (resume-partitions consumer queue-name)
                                               (reset! paused-partitions nil))
                                             (catch WakeupException _)
                                             (catch InterruptedException _)
                                             (catch Exception ex
                                               (let [now (System/currentTimeMillis)]
                                                 (when (or (zero? @last-log-time) (> (- @last-log-time now) 10000))
                                                   (log/error ex "Queue error" queue-name)
                                                   (reset! last-log-time now)))
                                               (Thread/sleep interval))
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
                          {:consumer consumer :future fu :paused? paused-partitions
                           :pending-msgs pending-msgs :partition->offset partition->offset}))
                      (map (range consumer-count))
                      (doall))]
    {:consumers consumers :stopped? stopped?}))

(defn stop-consumers [subscribed-consumers]
  (when-not (empty? subscribed-consumers)
    (let [{:keys [consumers stopped?]} subscribed-consumers]
      (reset! stopped? true)
      (doseq [{:keys [consumer ^Future future]} consumers]
        (.wakeup ^KafkaConsumer consumer)
        (when (and future (not (.isDone future)))
          (.cancel future true)))
      (reify java.util.concurrent.Future
        (get [_] (do (doseq [{:keys [future]} consumers]
                       (try
                         (some-> future (deref))
                         (catch Exception _)))
                     "OK"))
        (get [_ timeout unit]
          (let [timeout-ms (.toMillis unit timeout)]
            (loop [remain-ms timeout-ms remain-consumers consumers]
              (when (and (>= remain-ms 0) (not-empty remain-consumers))
                (let [start (System/currentTimeMillis)
                      cost  (when-let [future (:future (first remain-consumers))]
                              (try
                                (deref future remain-ms nil)
                                (- (System/currentTimeMillis) start)
                                (catch Exception _)))]
                  (recur (long (- remain-ms (or cost 0))) (rest remain-consumers)))))
            "OK"))
        (isCancelled [_] false)
        (isDone [_] (not (some #(and (:future %) (not (.isDone ^Future (:future %)))) consumers)))
        (cancel [_ interrupt?])))))
