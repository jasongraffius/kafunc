(ns kafunc.local
  "Functions which can provide a local Kafka server."
  (:require
    [kafunc.core :refer [*kafka-connect*]]
    [kafunc.impl.interop.scala :as scala]
    [kafunc.impl.interop :as interop])
  (:import
    (kafka.server KafkaConfig KafkaServer)
    (org.apache.zookeeper.server ZooKeeperServer ServerCnxnFactory)
    (org.apache.kafka.common.utils Time)
    (kafka.cluster EndPoint)
    (java.util Properties)))

(def ^:dynamic *zookeeper-connect*
  "The connection info for the running kafka server. If no such server is
  running, the value is nil. Should be set by the 'with-zookeeper' macro."
  nil)

(defn make-kafka-props
  "Merge any user properties with default values for Kafka.

  Args:
    * config   - map of configuration properties
    * temp-dir - temporary directory to use if :log-dir isn't specified"
  [config temp-dir]
  (let [default {:log-dir           (str temp-dir)
                 :port              "0"
                 :zookeeper-connect *zookeeper-connect*}]
    (interop/property-map->properties (merge default config))))

(defn make-kafka-config
  "Make a KafkaConfig object from the specified properties.

  Args:
    * props - Property map (String: String)."
  [props] (KafkaConfig. props))

(defn setup-kafka
  "Set up a Kafka server instance.

  Args:
    * config - KafkaConfig object."
  [^KafkaConfig config]
  (KafkaServer. config Time/SYSTEM scala/none scala/empty-list))

(defn setup-zookeeper
  "Set up a zookeeper instance."
  []
  (let [tmp (interop/temp-dir "zookeeper")]
    (ZooKeeperServer. tmp tmp ZooKeeperServer/DEFAULT_TICK_TIME)))

(defn kafka-port
  "Get the port from the running kafka server.

  Args:
    server - KafkaServer instance."
  [^KafkaServer server ^KafkaConfig config]
  (let [endpoints (scala/seq->list (.advertisedListeners config))
        ports     (map #(.boundPort server (.listenerName %)) endpoints)]
    (first ports)))

(defmacro with-zookeeper
  "Start a zookeeper instance in the same process,"
  [& body]
  `(let [zookeeper# (setup-zookeeper)
         conn#      (ServerCnxnFactory/createFactory 0 0)]
     ;; Startup Zookeeper service
     (.startup conn# zookeeper#)
     (try
       ;; Set the binding within the body
       (binding [*zookeeper-connect* (str "localhost:" (.getLocalPort conn#))]
         ~@body)
       (finally
         ;; Shutdown the Zookeeper zervice
         (.shutdown conn#)))))

(defmacro with-kafka
  "Create a local kafka service. Sets the *kafka-connect* variable within its
   context."
  [opt-config & body]
  (let [present? (or (map? opt-config) (nil? opt-config))
        host     "localhost"]
    `(let [temp-dir# (interop/temp-dir "kafka")
           props#    (make-kafka-props ~(if present? opt-config nil) temp-dir#)
           config#   (make-kafka-config props#)
           kafka#    (setup-kafka config#)]
       ;; Start the Kafka service
       (.startup kafka#)
       (try
         ;; Set the binding within the body
         (binding [*kafka-connect* (str ~host \: (kafka-port kafka# config#))]
           ;; If optional-config was not in fact a config, include it as the
           ;; first line of the body.
           ~@(if present? body (cons opt-config body)))
         (finally
           ;; Shutdown the Kafka service
           (.shutdown kafka#)
           (.awaitShutdown kafka#)
           ;; If a temporary directory was created, delete it
           (when (= (str temp-dir#) (get props# "log.dir"))
             (interop/unlink temp-dir# :recursive :silent)))))))
