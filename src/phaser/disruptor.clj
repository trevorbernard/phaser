(ns phaser.disruptor
  (:import [com.lmax.disruptor ClaimStrategy EventFactory EventHandler
            EventProcessor EventTranslator ExceptionHandler RingBuffer
            WaitStrategy Sequence SequenceBarrier BatchEventProcessor
            EventPublisher WorkHandler WorkerPool]
           [com.lmax.disruptor.dsl Disruptor EventHandlerGroup]
           [java.util.concurrent ExecutorService]))

(defn ^EventFactory event-factory* [handler]
  (reify EventFactory
    (newInstance [_]
      (handler))))

(defmacro event-factory [& args]
  `(event-factory* (fn ~@args)))

(defn ^EventHandler event-handler* [handler]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [_ event sequence end-of-batch?]
      (handler event sequence end-of-batch?))))

(defmacro event-handler [& args]
  `(event-handler* (fn ~@args)))

(defn ^EventTranslator event-translator* [handler]
  (reify com.lmax.disruptor.EventTranslator
    (translateTo [_ event sequence]
      (handler event sequence))))

(defmacro event-translator [& args]
  `(event-translator* (fn ~@args)))

(defn ^ExceptionHandler exception-handler
  [on-event on-start on-shutdown]
  (reify com.lmax.disruptor.ExceptionHandler
    (handleEventException [_ exception sequence event]
      (on-event exception sequence event))
    (handleOnStartException [_ exception]
      (on-start exception))
    (handleOnShutdownException [_ exception]
      (on-shutdown exception))))

(defn disruptor
  ([^EventFactory factory size ^ExecutorService executor]
     (Disruptor. factory (int size) executor))
  ([^EventFactory factory ^ExecutorService executor ^ClaimStrategy claim ^WaitStrategy wait]
     (Disruptor. factory executor claim wait)))

(defn publish-event [^Disruptor disruptor ^Object event]
  (.publishEvent disruptor event))

(defn dispatch-fn [_ & handlers]
  (when (seq handlers)
    (type (first handlers))))

(defmulti handle-events-with dispatch-fn)

(defmethod handle-events-with EventHandler [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventHandler handlers)))

(defmethod handle-events-with EventProcessor [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventProcessor handlers)))

(defmethod handle-events-with :default [_ _]
  (throw (Exception. "Unsupported handle-event-with dispatch type")))

(defmulti then dispatch-fn)

(defmethod then EventHandler [^Disruptor disruptor & handlers]
  (.then disruptor (into-array EventHandler handlers)))

(defmethod then EventProcessor [^Disruptor disruptor & handlers]
  (.then disruptor (into-array EventProcessor  handlers)))

(defmethod then :default [_ _]
  (throw (Exception. "Unsupported then dispatch type")))

(defmulti after dispatch-fn)

(defmethod after EventHandler [^Disruptor disruptor & handlers]
  (.after disruptor (into-array EventHandler handlers)))

(defmethod after EventProcessor [^Disruptor disruptor & handlers]
  (.after disruptor (into-array EventProcessor  handlers)))

(defmethod after :default [_ _]
  (throw (Exception. "Unsupported then dispatch type")))

(defn handle-exceptions-with [^Disruptor disruptor ^ExceptionHandler exception-handler]
  (.handleExceptionsWith disruptor exception-handler))

(defn handle-exceptions-for [^Disruptor disruptor ^EventHandler event-handler]
  (.handleExceptionsFor disruptor event-handler))

(defn ^RingBuffer start [^Disruptor disruptor]
  (.start disruptor))

(defn halt [^Disruptor disruptor]
  (.halt disruptor))

(defn shutdown [^Disruptor disruptor]
  (.shutdown disruptor))

(defn get-ring-buffer [^Disruptor disruptor]
  (.getRingBuffer disruptor))

(defn get-cursor [^Disruptor disruptor]
  (.getCursor disruptor))

(defn get-buffer-size [^Disruptor disruptor]
  (.getBufferSize disruptor))

(defn ^RingBuffer create-ring-buffer
  ([^EventFactory factory ^Integer buffer-size]
     (RingBuffer. factory buffer-size))
  ([^EventFactory factory ^ClaimStrategy claim-strategy ^WaitStrategy wait-strategy]
     (RingBuffer. factory claim-strategy wait-strategy)))

(defn ^SequenceBarrier create-sequence-barrier
  [^RingBuffer ring-buffer & sequences]
  (.newBarrier ring-buffer (into-array Sequence sequences)))

(defn set-gating-sequences
  [^RingBuffer ring-buffer & sequences]
  (.setGatingSequences ring-buffer (into-array Sequence sequences)))

(defn ^BatchEventProcessor create-batch-event-processor
  [^RingBuffer ring-buffer ^SequenceBarrier barrier ^EventHandler handler]
  (BatchEventProcessor. ring-buffer barrier handler))

(defn stop-event-processor
  [^BatchEventProcessor processor]
  (.halt processor))

(defn ^EventPublisher create-event-publisher
  [^RingBuffer ring-buffer]
  (EventPublisher. ring-buffer))

(defn send-event-with-publisher
  [^EventPublisher publisher ^EventTranslator translator]
  (.publishEvent publisher translator))

(defn ^WorkHandler work-handler* [handler]
  (reify com.lmax.disruptor.WorkHandler
    (onEvent [_ event]
      (handler event))))

(defmacro work-handler [& args]
  `(work-handler* (fn ~@args)))

(defn worker-pool
  ([^EventFactory factory ^ClaimStrategy claim ^WaitStrategy wait ^ExceptionHandler exception handlers]
   (WorkerPool. factory claim wait exception (into-array WorkHandler handlers)))
  ([^RingBuffer ring ^SequenceBarrier barrier ^ExceptionHandler exception handlers]
   (WorkerPool. ring barrier exception (into-array WorkHandler handlers))))
