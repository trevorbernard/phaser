(ns phaser.examples
  (:refer-clojure :exclude [and])
  (:use
   phaser.disruptor
   phaser.dsl)
  (:import
   java.util.concurrent.Executors
   [com.lmax.disruptor EventHandler]
   [com.lmax.disruptor.dsl Disruptor]))

(defprotocol IMessage
  (getValue [_])
  (setValue [_ v]))

(deftype Message [^:unsynchronized-mutable value]
  IMessage
  (getValue [_] value)
  (setValue [_ v] (set! value v)))

(deffactory message-factory []
  (Message. nil))

(defhandler journaller
  [^Message event ^long sequence end-of-batch?]
  (println "Journalling..."))

(defhandler business-logic
  [^Message event ^long sequence end-of-batch?]
  (println "Getting work done..."))

(deftranslator translator
  [^Message event ^long sequence]
  (println "translating...")
  (setValue event "Ohai"))

(defn wire-up-disruptor [exec]
  (let [disruptor (create-disruptor message-factory 1024 exec)]
    (-> disruptor
        (handle-events-with journaller)
        (then business-logic))
    (let [rb (start disruptor)]
      [disruptor (create-event-publisher rb translator)])))

(defn -main []
  (let [exec (Executors/newCachedThreadPool)
        [disruptor publisher] (wire-up-disruptor exec)]
    (publisher)
    (publisher)
    (shutdown disruptor)
    (.shutdown exec)))
