(defproject kafunc/kafunc "0.2.0-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :url "https://github.com/kafunc/kafunc"
  :description "A less-imperative approach to Kafka."
  :dependencies [[org.apache.kafka/kafka_2.11 "0.10.2.1"]
                 [org.clojure/clojure "1.8.0"]]
  :repl-options {:init-ns kafunc.core})
