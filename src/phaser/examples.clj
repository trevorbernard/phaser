;; Copyright 2013 UserEvents Inc.
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

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
