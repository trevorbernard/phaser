(ns phaser.disruptor
  (:import [com.lmax.disruptor EventFactory EventTranslator EventHandler
            ClaimStrategy WaitStrategy MultiThreadedClaimStrategy
            SingleThreadedClaimStrategy BlockingWaitStrategy
            SleepingWaitStrategy YieldingWaitStrategy BusySpinWaitStrategy
            EventProcessor RingBuffer]
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

(defn disruptor [^EventFactory factory ^ExecutorService executor size ]
  (Disruptor. factory executor (int size)))

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
