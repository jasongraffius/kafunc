(ns kafunc.impl.interop
  "Namespace for interop with Java/Kafka, to keep core as pure clojure"
  (:refer-clojure :exclude [send])
  (:require [kafunc.impl.util :as util]
            [clojure.java.io :as jio])
  (:import (org.apache.kafka.clients.consumer
             KafkaConsumer ConsumerRecord Consumer)
           (org.apache.kafka.clients.producer
             KafkaProducer ProducerRecord Producer)
           (org.apache.kafka.common.serialization
             ByteArraySerializer ByteArrayDeserializer)
           (java.util Properties UUID)
           (java.io
             ByteArrayInputStream ObjectInputStream
             ByteArrayOutputStream ObjectOutputStream File)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(def byte-serializer (.getName ByteArraySerializer))
(def byte-deserializer (.getName ByteArrayDeserializer))

(defrecord CRecord [key value partition topic timestamp offset checksum])
(defrecord PRecord [key value partition topic timestamp])

(defn property-map->properties
  ^Properties [m]
  (doto (Properties.)
    (.putAll (util/map->properties m))))

(defn kafka->crecord
  "Convert a Kafka ConsumerRecord to a Clojure record"
  [^ConsumerRecord record]
  (->CRecord
    (.key record)
    (.value record)
    (.partition record)
    (.topic record)
    (.timestamp record)
    (.offset record)
    (.checksum record)))

(defn kafka->precord
  "Convert a Kafka ProducerRecord to a Clojure record"
  [^ProducerRecord record]
  (->PRecord
    (.key record)
    (.value record)
    (.partition record)
    (.topic record)
    (.timestamp record)))

(defn precord->kafka
  "Convert a Clojure record (PRecord) to a Kafka ProducerRecord"
  [record]
  (ProducerRecord.
    (:topic record)
    (:partition record)
    (:timestamp record)
    (:key record)
    (:value record)))

(defn record-meta->map
  [record-meta]
  {:topic     (.topic record-meta)
   :partition (.partition record-meta)
   :timestamp (.timestamp record-meta)
   :offset    (.offset record-meta)
   :checksum  (.checksum record-meta)})

(defn poll
  "Poll a Kafka consumer"
  [^Consumer consumer timeout]
  (when-let [polled (.poll consumer timeout)]
    (map kafka->crecord polled)))

(defn subscribe
  [consumer topics]
  "Subscribe a Kafka consumer to topics"
  (.subscribe consumer topics)
  ;; Return modified consumer
  consumer)

(defn make-consumer
  [config]
  (KafkaConsumer. (property-map->properties config)))

(defn make-producer
  [config]
  (KafkaProducer. (property-map->properties config)))

(defn consumer-subscriptions
  [^Consumer consumer]
  (let [subs (.subscription consumer)]
    (when (not (empty? subs)) subs)))

(defn send
  "Send a Kafka ProducerRecord or Clojure PRecord through a Kafka producer

  Returns the future that the producer generates."
  [^Producer producer record]
  (if (map? record)
    (recur producer (precord->kafka record))
    (.send producer record)))

(defn io-deserialize
  "A basic deserializer which uses java.io's deserialization."
  [bytes]
  (with-open [byte-stream   (ByteArrayInputStream. bytes)
              object-stream (ObjectInputStream. byte-stream)]
    (.readObject object-stream)))

(defn io-serialize
  "A basic serializer which uses java.io's serialization."
  [object]
  (with-open [byte-stream (ByteArrayOutputStream.)
              object-stream (ObjectOutputStream. byte-stream)]
    (.writeObject object-stream object)
    (.flush object-stream)
    (.toByteArray byte-stream)))

(defn unique-string [] (str (UUID/randomUUID)))

(defn temp-dir
  ^File [prefix & file-attrs]
  (.toFile
    (Files/createTempDirectory prefix (into-array FileAttribute file-attrs))))

(defn unlink
  [^File path & opts]
  (let [{recursive? :recursive, silent? :silent} (set opts)]
    (if recursive?
      (doseq [file (reverse (file-seq path))]
        (jio/delete-file file silent?))
      (jio/delete-file path silent?))))
