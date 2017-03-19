# clj-kafka-client

A Clojure library for [Kafka](https://kafka.apache.org/) new client API.

Development is against the 0.9.0 or newer release of Kafka.

## Usage

### Producer

```clojure
(use 'clj-kafka-client.producer)

(def kafka-producer (producer {"bootstrap.servers" "localhost:9092"}))

(send-record kafka-producer (record "My-Topic" {:msg "Hello world"}))
```

### Consumer

```clojure
(use 'clj-kafka-client.consumer)

(def consumer-with-worker (start-consuming-queue {"bootstrap.servers" "localhost:9092"
                                                  "group.id"          "My-Group"}
                                                 "My-Topic"
                                                 (fn [msg]
                                                   (println msg))))
```

`start-consuming-queue` will create a kafka consumer to subscribe topic "My-Topic" and will create a worker pool to process all the consumed msgs.

## License

Copyright © 2017 ylgrgyq

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
