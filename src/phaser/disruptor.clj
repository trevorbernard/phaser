(ns phaser.disruptor
    (:refer-clojure :exclude [get])
  (:import [com.lmax.disruptor EventFactory EventTranslator EventHandler
            ClaimStrategy WaitStrategy MultiThreadedClaimStrategy
            SingleThreadedClaimStrategy BlockingWaitStrategy
            SleepingWaitStrategy YieldingWaitStrategy BusySpinWaitStrategy
            EventProcessor RingBuffer ExceptionHandler]
           [com.lmax.disruptor.dsl Disruptor]
           [java.util.concurrent ExecutorService]))

(def claim-strategy
  {:single-threaded (fn [size] (SingleThreadedClaimStrategy. (int size)))
   :multi-threaded  (fn [size] (MultiThreadedClaimStrategy. (int size)))})

(def wait-strategy
  {:block (fn [] (BlockingWaitStrategy.))
   :yield (fn [] (YieldingWaitStrategy.))
   :sleep (fn [] (SleepingWaitStrategy.))
   :spin  (fn [] (BusySpinWaitStrategy.))})

(defn event-factory* [handler]
  (reify EventFactory
    (newInstance [_]
      (handler))))

(defmacro event-factory [& args]
  `(event-factory* (fn [] ~@args)))

(defn event-handler* [handler]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [_ event sequence end-of-batch?]
      (handler event sequence end-of-batch?))))

(defmacro event-handler [& args]
  `(event-handler* (fn [] ~@args)))

(defn event-translator* [handler]
  (reify com.lmax.disruptor.EventTranslator
    (translateTo [_ event sequence]
      (handler event sequence))))

(defmacro event-translator [& args]
  `(event-translator (fn [] ~@args)))

(defn ^ExceptionHandler exception-handler
  [on-event on-start on-shutdown]
  (reify com.lmax.disruptor.ExceptionHandler
    (handleEventException [_ exception sequence event]
      (on-event exception sequence event))
    (handleOnStartException [_ exception]
      (on-start exception))
    (handleOnShutdownException [_ exception]
      (on-shutdown exception))))

(defn disruptor [^EventFactory factory size ^ExecutorService executor]
  (Disruptor. factory (int size) executor))

(defn publish-event [^Disruptor disruptor ^Object event]
  (.publishEvent disruptor event))

(defn dispatch-fn [_ handlers]
  (when (seq handlers)
    (type (first handlers))))

(defmulti handle-events-with dispatch-fn)

(defmethod handle-events-with EventHandler [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventHandler handlers)))

(defmethod handle-events-with EventProcessor [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventProcessor  handlers)))

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

(defmethod then :default [_ _]
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

(defn get [^Disruptor disruptor ^long sequence]
  (.get disruptor sequence))
