(ns kafunc.impl.interop.scala
  "Scala-specific interop. These functions are used in the local Kafka server
  related features only. Only the bare minimum is added here to allow for the
  feature."
  (:import
    (scala Option)
    (scala.collection.immutable List$)
    (scala.collection.convert WrapAsJava$)))

(def none
  "Scala's ``None`` type for ``scala.Option`` values."
  (Option/apply nil))

(def empty-list
  "An empty ``scala.collection.immutable.List``."
  (.empty (List$/MODULE$)))

(defn seq->list
  "Convert a ``scala.collection.Seq`` to a clojure vector."
  [s]
  (into [] (.seqAsJavaList (WrapAsJava$/MODULE$) s)))
