(ns phaser.dsl
  "A concise syntax for setting up the disruptor pattern around a RingBuffer.

  A simple example of setting up the disruptor with two event handlers that
  must process events in order:"
  (:refer-clojure :exclude [and])
  (:import
   [com.lmax.disruptor EventFactory EventHandler ExceptionHandler
    RingBuffer WaitStrategy EventTranslator EventProcessor SequenceBarrier
    WorkHandler]
   [com.lmax.disruptor.dsl Disruptor EventHandlerGroup ProducerType]
   [java.util.concurrent Executor]))

(defn- ^ProducerType convert-type [key]
  (condp = key
    :multi ProducerType/MULTI
    :single ProducerType/SINGLE))

(defn ^Disruptor disruptor
  "Create a new disruptor pattern"
  ([^EventFactory event-factory size ^Executor exec]
     (Disruptor. event-factory (int size) exec))
  ([^EventFactory event-factory size ^Executor exec producer-type
    ^WaitStrategy wait-strategy]
     (Disruptor. event-factory (int size) exec (convert-type producer-type)
                 wait-strategy)))

(defn ^RingBuffer start
  "Starts the event processors and returns the fully configured RingBuffer.
  The RingBuffer is set up to prevent overwriting any entry that is yet to be
  processed by the slowest event processor. This method must only be called
  one after all the event processors have been added."
  [^Disruptor disruptor]
  (.start disruptor))

(defn shutdown
  "Waits until all events currently in the disruptor have been processed by all
  event processors and then halts the processors."
  [^Disruptor disruptor]
  (.shutdown disruptor))

(defn halt
  "Calls EvenProcessor.halt() on all of the event processors created via this
  Disruptor."
  [^Disruptor disruptor]
  (.halt disruptor))

(defn get-ring-buffer
  "The RingBuffer used by this Disruptor."
  [^Disruptor disruptor]
  (.getRingBuffer disruptor))

(defn dispatch-fn [_ & handlers]
  (when (seq handlers)
    (class (first handlers))))

(defmulti handle-events-with
  "Set up batch handlers to handle events from the ring buffer."
  dispatch-fn)

(defmethod ^EventHandlerGroup handle-events-with EventHandler
  [clazz & handlers]
  (.handleEventsWith clazz (into-array EventHandler handlers)))

(defmethod ^EventHandlerGroup handle-events-with EventProcessor
  [clazz & processors]
  (.handleEventsWith clazz (into-array EventProcessor processors)))

(defmethod handle-events-with :default
  [& args]
  (throw
   (IllegalArgumentException. "Unsupported arguments for handles-events-with")))

(defmulti handle-events-with-worker-pool
  "Set up a worker pool to handle events from the ring buffer."
  dispatch-fn)

(defmethod handle-events-with-worker-pool WorkHandler
  [clazz & handlers]
  (.handleEventsWithWorkerPool clazz (into-array WorkHandler handlers)))

(defmulti after
  "Create a group of event handlers/processors to be used as a dependency."
  dispatch-fn)

(defmethod ^EventHandlerGroup after EventHandler
  [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventHandler handlers)))

(defmethod ^EventHandlerGroup after EventProcessor
  [^Disruptor disruptor & processors]
  (.handleEventsWith disruptor (into-array EventProcessor processors)))

(defmethod after :default
  [& args]
  (throw (IllegalArgumentException. "Unsupported arguments for after")))

(defn ^SequenceBarrier get-barrier-for [^Disruptor disruptor & handlers]
  (.getBarrierFor disruptor (into-array EventHandler handlers)))

(defn get-buffer-size [^Disruptor disruptor]
  (.getBufferSize disruptor))

(defn get-cursor [^Disruptor disruptor]
  (.getCursor disruptor))

(defn handle-exceptions-with [^Disruptor disruptor
                              ^ExceptionHandler exception-handler]
  (.handleExceptionsWith disruptor exception-handler))

(defmulti and (fn [_ b] (if (seq b)
                          (class (first b))
                          (class b))))

(defmethod ^EventHandlerGroup and EventHandlerGroup [a ^EventHandlerGroup other]
  (.and a other))

(defmethod ^EventHandlerGroup and EventProcessor [a ^EventProcessor other]
  (.and a other))

(defmethod and :default
  [& args]
  (throw (IllegalArgumentException. "Unsupported arguments for and")))

(defn as-sequence-barrier [^EventHandlerGroup group]
  (.asSequenceBarrier group))

(defn ^EventHandlerGroup then [^EventHandlerGroup group & handlers]
  (.then group (into-array EventHandler handlers)))
