[![Clojars Project](https://img.shields.io/clojars/v/kafunc.svg)](https://clojars.org/kafunc)

# Kafunc
## A less imperative approach to Kafka

Kafunc is a library to provide a means to interact with Kafka topics as if they
were a Clojure ``seq``. They are lazily created by polling a consumer, and
further polls are only done when the end of that seq is reached. Similarly, a
``seq`` of records can be sent to various topics/partitions.
