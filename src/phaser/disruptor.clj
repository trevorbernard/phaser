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

(ns phaser.disruptor
  "High Performance Inter-Thread Messaging Library"
  (:use
   [clojure.tools.macro :only [name-with-attributes]])
  (:import
   [com.lmax.disruptor EventFactory EventHandler EventTranslator
    EventTranslatorOneArg EventTranslatorTwoArg EventTranslatorThreeArg
    EventTranslatorVararg ExceptionHandler RingBuffer WorkHandler
    Sequence SequenceBarrier WaitStrategy BatchEventProcessor WorkerPool
    EventProcessor SequenceGroup]
   [com.lmax.disruptor.dsl Disruptor]
   [java.util.concurrent ExecutorService]))

(defn ^EventFactory create-event-factory
  "Create an EventFactory which is called by the RingBuffer to pre-populate all
   the events to fill the RingBuffer."
  [handler]
  (reify EventFactory
    (newInstance [_]
      (handler))))

(defmacro deffactory
  "Define a Disruptor EventFactory which is called by the RingBuffer."
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deffactory requires a vector for its bindings")))
    (when-not (zero? (count bindings))
      (throw
       (IllegalArgumentException.
        "deffactory requires a binding with no parameters")))
    `(def ~name (create-event-factory (fn ~bindings ~@args)))))

(defn ^EventHandler create-event-handler
  "Create an EventHandler which is a callback interface for processing events
  as they become available in the RingBuffer."
  [handler]
  (reify EventHandler
    (onEvent [_ event sequence end-of-batch?]
      (handler event sequence end-of-batch?))))

(defmacro defhandler
  "Define a Disruptor event handler which is a callback interface for processing
  events as they become available in the RingBuffer."
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "defhandler requires a vector for its bindings")))
    (when-not (= 3 (count bindings))
      (throw
       (IllegalArgumentException.
        "defhandler requires a binding with 3 parameters")))
    `(def ~name (create-event-handler (fn ~bindings ~@args)))))


(defn ^WorkHandler create-work-handler
  "Create a WorkHandler which is for processing units of work as they become
  available in the RingBuffer."
  [handler]
  (reify WorkHandler
    (onEvent [_ event]
      (handler event))))

(defn ^EventTranslator create-event-translator
  "Create an EventTranslator which translate (write) data representations into
  events claimed from the RingBuffer"
  [handler]
  (reify EventTranslator
    (translateTo [_ event sequence]
      (handler event sequence))))

(defn ^EventTranslatorOneArg create-event-translator-one-arg
  "Create an EventTranslatorOneArg which translate (write) data representations
  into events claimed from the RingBuffer"
  [handler]
  (reify EventTranslatorOneArg
    (translateTo [_ event sequence arg0]
      (handler event sequence arg0))))

(defn ^EventTranslatorTwoArg create-event-translator-two-arg
  "Create an EventTranslatorTwoArg which translate (write) data representations
  into events claimed from the RingBuffer"
  [handler]
  (reify EventTranslatorTwoArg
    (translateTo [_ event sequence arg0 arg1]
      (handler event sequence arg0 arg1))))

(defn ^EventTranslatorThreeArg create-event-translator-three-arg
  "Create an EventTranslatorThreeArg which translate (write) data
  representations into events claimed from the RingBuffer"
  [handler]
  (reify EventTranslatorThreeArg
    (translateTo [_ event sequence arg0 arg1 arg2]
      (handler event sequence arg0 arg1 arg2))))

(defn ^EventTranslatorVararg create-event-translator-var-arg
  "Create an EventTranslatorVararg which translate (write) data representations
  into events claimed from the RingBuffer. The Handler should accept a variadic
  number of params"
  [handler]
  (reify EventTranslatorVararg
    (translateTo [_ event sequence args]
      (apply handler event sequence args))))

(defmacro deftranslator
  "Define a Disrupter EventTranslator which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deftranslator requires a vector for its bindings")))
    (when-not (= 2 (count bindings))
      (throw
       (IllegalArgumentException.
        "deftranslator requires a binding with 2 parameters")))
    `(def ~name (create-event-translator (fn ~bindings ~@args)))))

(defmacro deftranslator1
  "Define a Disrupter EventTranslatorOneArg which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deftranslator1 requires a vector for its bindings")))
    (when-not (= 3 (count bindings))
      (throw
       (IllegalArgumentException.
        "deftranslator1 requires a binding with 3 parameters")))
    `(def ~name (create-event-translator-one-arg (fn ~bindings ~@args)))))

(defmacro deftranslator2
  "Define a Disrupter EventTranslatorTwoArg which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deftranslator2 requires a vector for its bindings")))
    (when-not (= 4 (count bindings))
      (throw
       (IllegalArgumentException.
        "deftranslator2 requires a binding with 4 parameters")))
    `(def ~name (create-event-translator-two-arg (fn ~bindings ~@args)))))

(defmacro deftranslator3
  "Define a Disrupter EventTranslatorThreeArg which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deftranslator3 requires a vector for its bindings")))
    (when-not (= 5 (count bindings))
      (throw
       (IllegalArgumentException.
        "deftranslator3 requires a binding with 5 parameters")))
    `(def ~name (create-event-translator-three-arg (fn ~bindings ~@args)))))

(defmacro deftranslatorvarg
  "Define a Disrupter EventTranslatorVararg which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        [bindings & args] args]
    (when-not (vector? bindings)
      (throw
       (IllegalArgumentException.
        "deftranslatorvarg requires a vector for its bindings")))
    (when-not (>= 2 (count bindings))
      (throw
       (IllegalArgumentException.
        "deftranslatorvarg requires a binding with at least 2 parameters")))
    `(def ~name (create-event-translator-varg (fn ~bindings ~@args)))))

(defn ^ExceptionHandler create-exception-handler
  [on-event on-start on-shutdown]
  (reify com.lmax.disruptor.ExceptionHandler
    (handleEventException [_ exception sequence event]
      (on-event exception sequence event))
    (handleOnStartException [_ exception]
      (on-start exception))
    (handleOnShutdownException [_ exception]
      (on-shutdown exception))))

(defmulti create-event-publisher
  "Returns a function that publishes events to the RingBuffer of different arity
  depending on which EventTranslator is being used."
  (fn [a b] (class b)))

(defmethod create-event-publisher EventTranslator
  [^RingBuffer rb ^EventTranslator translator]
  (fn []
    (.publishEvent rb translator)))

(defmethod create-event-publisher EventTranslatorOneArg
  [^RingBuffer rb ^EventTranslatorOneArg translator]
  (fn [arg0]
    (.publishEvent rb translator arg0)))

(defmethod create-event-publisher EventTranslatorTwoArg
  [^RingBuffer rb ^EventTranslatorTwoArg translator]
  (fn [arg0 arg1]
    (.publishEvent rb translator arg0 arg1)))

(defmethod create-event-publisher EventTranslatorThreeArg
  [^RingBuffer rb ^EventTranslatorThreeArg translator]
  (fn [arg0 arg1 arg2]
    (.publishEvent rb translator arg0 arg1 arg2)))

(defmethod create-event-publisher EventTranslatorVararg
  [^RingBuffer rb ^EventTranslatorVararg translator]
  (fn [args]
    (.publishEvent rb translator (object-array args))))

(defn add-gating-sequences
  "Add the specified gating sequences to this instance of the Disruptor. They
  will safely and atomically added to the list of gating sequences."
  [^RingBuffer rb sequences]
  (.addGatingSequences rb (into-array Sequence sequences)))

(defn create-multi-producer
  ([^EventFactory factory size]
     (RingBuffer/createMultiProducer factory (int size)))
  ([^EventFactory factory size ^WaitStrategy strategy]
     (RingBuffer/createMultiProducer factory (int size) strategy)))

(defn create-single-producer
  ([^EventFactory factory size]
     (RingBuffer/createSingleProducer factory (int size)))
  ([^EventFactory factory size ^WaitStrategy strategy]
     (RingBuffer/createSingleProducer factory (int size) strategy)))

(defn event-for-sequence
  "Returns the event for a given sequence in the Ring Buffer"
  [^RingBuffer rb ^long sequence]
  (.get rb sequence))

(defn get-cursor
  [^RingBuffer rb]
  (.getCursor rb))

(defn get-buffer-size
  [^RingBuffer rb]
  (.getBufferSize rb))

(defn ^Sequence get-sequence
  [^EventProcessor event-processor]
  (.getSequence event-processor))

(defn add-sequence
  [^SequenceGroup sequence-group ^Sequence sequence]
  (.add sequence-group sequence))

(defn ^SequenceBarrier create-sequence-barrier
  [^RingBuffer rb sequences]
  (.newBarrier rb (into-array Sequence sequences)))

(defn ^BatchEventProcessor create-batch-event-processor
  [^RingBuffer rb ^SequenceBarrier barrier ^EventHandler handler]
  (BatchEventProcessor. rb barrier handler))

(defn ^BatchEventProcessor handle-exceptions-with
  [^BatchEventProcessor batch-processor ^ExceptionHandler exception-handler]
  (doto batch-processor
    (.setExceptionHandler exception-handler)))

(defn create-worker-pool
  ([^EventFactory factory ^ExceptionHandler exception-handler handlers]
     (WorkerPool. factory exception-handler (into-array EventHandler handlers)))
  ([^RingBuffer rb ^SequenceBarrier sb ^ExceptionHandler exception-handler
    handlers]
     (WorkerPool. rb sb exception-handler (into-array ExceptionHandler
                                                      handlers))))
