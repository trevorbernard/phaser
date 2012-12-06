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
   :multi-threaded (fn [size] (MultiThreadedClaimStrategy. (int size)))})

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

(defmulti handle-events-with #(class %2))

;; Lame/non-elegant hack to get array type for dispatching
(def EventHandlerArray (Class/forName "[Lcom.lmax.disruptor.EventHandler;"))
(def EventProcessorArray (Class/forName "[Lcom.lmax.disruptor.EventProcessor;"))

(defmethod handle-events-with EventHandlerArray [^Disruptor disrupter & handlers]
  (.handleEventsWith disruptor (into-array EventHandler handlers)))

(defmethod handle-events-with EventProcessorArray [^Disruptor disrupter & handlers]
  (.handleEventsWith disruptor (into-array EventProcessor  handlers)))

(defn handle-events-with* [^Disruptor disruptor & handlers]
  (.handleEventsWith disruptor (into-array EventHandler handlers)))

(defmulti then #(class %2))

(defmethod then EventHandlerArray [^Disruptor disrupter & handlers]
  (.then disruptor (into-array EventHandler handlers)))

(defmethod then EventProcessorArray [^Disruptor disrupter & handlers]
  (.then disruptor (into-array EventProcessor  handlers)))

(defn then* [^Disruptor disruptor & handlers]
  (into-array)
  (.then disruptor (into-array EventHandler handlers)))
