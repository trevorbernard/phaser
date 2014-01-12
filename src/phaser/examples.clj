;; Copyright 2013-2014 UserEvents Inc.
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
  (:require [phaser
             [disruptor
              :refer [defhandler deftranslator]
              :as disruptor]
             [dsl :as dsl]]))

(defprotocol IMessage
  (getValue [_])
  (setValue [_ v]))

(deftype Message [^:unsynchronized-mutable value]
  IMessage
  (getValue [_] value)
  (setValue [_ v] (set! value v)))

(def message-factory
  (disruptor/create-event-factory #(->Message nil)))

;;The defhandler macro is useful when the defined macro does not require
;;internal state
(defhandler journaller
  [event sequence end-of-batch?]
  (println "Journalling..." (getValue event)))

;;When internal state is required in a handler, a little more work
;;is required. The event handler must be wrapped in order to contain
;;the state in a closure. For maximum speed you can define a mutable
;;type similar to the Message above and enclose an instantiation of
;;the type.
(defn create-business-logic
  []
  (let [state (atom 0)]
    (disruptor/create-event-handler
     (fn [event sequence end-of-batch?]
       (println "Getting work done..."
                (swap! state inc))))))

(deftranslator translator
  [event sequence]
  (println "translating...")
  (setValue event "Ohai"))

(defn wire-up-disruptor [exec]
  (let [disruptor (dsl/create-disruptor message-factory 1024 exec)]
    (-> disruptor
        (dsl/handle-events-with journaller)
        (dsl/then (create-business-logic)))
    (let [rb (dsl/start disruptor)]
      [disruptor (disruptor/create-event-publisher rb translator)])))

(defn -main []
  (let [exec (java.util.concurrent.Executors/newCachedThreadPool)
        [disruptor publisher] (wire-up-disruptor exec)]
    (publisher)
    (publisher)
    (dsl/shutdown disruptor)
    (.shutdown exec)))

;; Sample output:
; $ lein run
; translating...
; translating...
; Journalling... Ohai
; Journalling... Ohai
; Getting work done... 1
; Getting work done... 2
