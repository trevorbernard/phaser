(ns phaser.disruptor
  (:require
   [clojure.tools.macro :refer [name-with-attributes]])
  (:import
   [com.lmax.disruptor EventFactory EventHandler EventTranslator
    EventTranslatorOneArg EventTranslatorTwoArg EventTranslatorThreeArg
    EventTranslatorVararg ExceptionHandler RingBuffer WorkHandler
    Sequence SequenceBarrier WaitStrategy BatchEventProcessor WorkerPool]
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
        args (if (vector? (first args))
               args
               (if (seq? (first args))
                 args
                 (throw (IllegalArgumentException.
                         (if (seq args)
                           (str "Parameter declaration "
                                (first args)
                                " should be a vector")
                           (str "Parameter declaration missing"))))))]
      (let [[name args] (name-with-attributes name args)]
        `(def ~name (create-event-factory* (fn ~@args))))))

(defn ^EventHandler create-event-handler
  "Create an EventHandler which is a callback interface for processing events
  as they become available in the RingBuffer."
  [handler]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [_ event sequence end-of-batch?]
      (handler event sequence end-of-batch?))))

(defmacro defhandler
  "Define a Disruptor event handler which is a callback interface for processing
  events as they become available in the RingBuffer."
  [name & args]
  (let [[name args] (name-with-attributes name args)
        args (if (vector? (first args))
               args
               (if (seq? (first args))
                 args
                 (throw (IllegalArgumentException.
                         (if (seq args)
                           (str "Parameter declaration "
                                (first args)
                                " should be a vector")
                           (str "Parameter declaration missing"))))))]
    (let [[name args] (name-with-attributes name args)]
      `(def ~name (create-event-handler (fn ~@args))))))


(defn ^WorkHandler create-work-handler
  "Create a WorkHandler which is for processing units of work as they become
  available in the RingBuffer"
  [handler]
  (reify WorkHandler
    (onEvent [_ event]
      (handler event))))

(defmacro work-handler [& args]
  `(work-handler* (fn ~@args)))

(defn create-event-translator
  "Create an EventTranslator which translate (write) data representations into
  events claimed from the RingBuffer"
  ([handler]
     (reify EventTranslator
       (translateTo [_ event sequence]
         (handler event sequence))))
  ([handler & [params]]
     (case (count params)
       1 (reify EventTranslatorOneArg
           (translateTo [_ event sequence arg0]
             (handler event sequence arg0)))
       2 (reify EventTranslatorTwoArg
           (translateTo [_ event sequence arg0 arg1]
             (handler event sequence arg0 arg1)))
       3 (reify EventTranslatorThreeArg
           (translateTo [_ event sequence arg0 arg1 arg2]
             (handler event sequence arg0 arg1 arg2)))
       (reify EventTranslatorVararg
         (translateTo [_ event sequence args]
           (apply handler event sequence args))))))

(defmacro deftranslator
  "Define a Disrupter EventTranslator which translate (write) data
  representations into events claimed from the RingBuffer"
  [name & args]
  (let [[name args] (name-with-attributes name args)
        args (if (vector? (first args))
               args
               (if (seq? (first args))
                 args
                 (throw (IllegalArgumentException.
                         (if (seq args)
                           (str "Parameter declaration "
                                (first args)
                                " should be a vector")
                           (str "Parameter declaration missing"))))))
        params (first args)]
    `(let [body# (fn ~@args)]
       (def ~name
         ((fn ~params (apply create-event-translator body# ~params)))))))

#_ (deftranslator morecowbell
     [event sequence a b]
     (println "Event:" event))

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

(defn add-gating-sequences [^RingBuffer rb sequences]
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

(defn get-sequence
  [rb ^long sequence]
  (.get rb sequence))

(defn get-cursor
  [rb ^long sequence]
  (.getCursor rb sequence))

(defn get-buffer-size
  [rb]
  (.getBufferSize rb))

(defn ^SequenceBarrier create-sequence-barrier
  [^RingBuffer rb sequences]
  (.newBarrier rb (into-array Sequence sequences)))

(defn ^BatchEventProcessor create-batch-event-processor
  [^RingBuffer rb ^SequenceBarrier barrier ^EventHandler handler]
  (BatchEventProcessor. rb barrier handler))

(defn create-worker-pool
  ([^EventFactory factory ^ExceptionHandler exception-handler handlers]
     (WorkerPool. factory exception-handler (into-array EventHandler handlers)))
  ([^RingBuffer rb ^SequenceBarrier sb ^ExceptionHandler exception-handler
    handlers]
     (WorkerPool. rb sb exception-handler (into-array ExceptionHandler
                                                      handlers))))
