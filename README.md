# clj-kafka-client

A Clojure library for [Kafka](https://kafka.apache.org/) new client API.

Development is against the 0.9.0 or newer release of Kafka.

## Usage

### Producer

```clojure
(use 'clj-kafka-client.producer)

(def kafka-producer (producer {"bootstrap.servers" "localhost:9092"}))

(let [r (record "My-Topic" "Hello world")  ; Build a record to send
      p (send-record kafka-producer r)]    ; Send the record to Kafka server and get a promise for this record
  @p)                                      ; Deref the promise to get the metadata when the record was acknowledged by server
```

The returned value of `send-record` implements the `java.util.concurrent.Future` interface. So if you like you can use it as a `Future` object instead of a promise in clojure.

### Consumer

You can use kafka consumer API directly with a shallow clojure wrapper like:
```clojure
(use 'clj-kafka-client.consumer)

(def kafka-consumer (consumer {"bootstrap.servers" "localhost:9092"
                               "group.id"          "My-Group"}))

(subscribe kafka-consumer)

(loop []
  (let [records (poll kafka-consumer)]
    (doseq [record records]
      (println record))))
```

But we recommend you to use high level API to consume Kafka topic. It helps you to do three dirty works. First,
create a kafka consumer subscribing to a topic; Second, create a thread pool to process consumed messages asynchronously
with the handler provided by you; Third, pause the assigned partition to prevent polling more messages
when consumer can not finish processing all the polled messages during a session timeout. And calling
poll periodically to ping Kafka server to keep alive during pausing time. Then finally resume polling messages
when all the previous polled messages is processed.

The third part is important and fallible due to the method `poll` in `KafkaConsumer` does two things: ping the
Kafka server to keep alive and poll new messages from Kafka server. If having not called `poll` over a session
 timeout the Kafka server will think the consumer is dead and start a unnecessary partition rebalance
which may lead to consume some messages more than once. But sometimes your handler maybe slow and can not process
all the polled messages within a session timeout. In this scenario you must call `pause` in `KafkaConsumer`
to let you call `poll` only to ping Kafka server without polling any new messages.

The high level API is as follows:
```clojure
(use 'clj-kafka-client.consumer)

(defn msg-handler [msg]
  (println msg))

(def kafka-consumer (create-high-level-consumer {"bootstrap.servers" "localhost:9092"
                                                 "group.id"          "My-Group"}
                                                "My-Topic"
                                                msg-handler))

;; Close consumer
(stop-high-level-consumer kafka-consumer)
```

## License

Copyright © 2017 ylgrgyq

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
