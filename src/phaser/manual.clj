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

(ns phaser.manual
  (:import
   [java.util.concurrent TimeUnit Executor]
   [com.lmax.disruptor RingBuffer WorkHandler DataProvider EventFactory EventHandler
    Sequence SequenceBarrier WaitStrategy BatchEventProcessor WorkerPool ExceptionHandler
    EventProcessor SequenceGroup WaitStrategy Cursored FatalExceptionHandler IgnoreExceptionHandler
    BlockingWaitStrategy BusySpinWaitStrategy PhasedBackoffWaitStrategy SleepingWaitStrategy
    TimeoutBlockingWaitStrategy YieldingWaitStrategy]))

;;Sometimes you just have to roll your own

(def time-units
  {:nano TimeUnit/NANOSECONDS
   :micro TimeUnit/MICROSECONDS
   :milli TimeUnit/MILLISECONDS
   :seconds TimeUnit/SECONDS
   :minutes TimeUnit/MINUTES
   :hours TimeUnit/HOURS
   :days TimeUnit/DAYS})

(defn create-exception-handler
  [type]
  (case type
    :fatal (FatalExceptionHandler.)
    :ignore (IgnoreExceptionHandler.)
    :none nil))

(defn ^WaitStrategy create-strategy
  [type opts]
  (case type
    :blocking (BlockingWaitStrategy.)
    :busy-spin (BusySpinWaitStrategy.)
    :phased-back-off-lock (let [{:keys [spin-timeout yield-timeout units]} opts]
                            (PhasedBackoffWaitStrategy/withLock spin-timeout
                                                                yield-timeout
                                                                (time-units units)))
    :phased-back-off-sleep (let [{:keys [spin-timeout yield-timeout units]} opts]
                             (PhasedBackoffWaitStrategy/withSleep spin-timeout
                                                                  yield-timeout
                                                                  (time-units units)))
    :sleeping (SleepingWaitStrategy.)
    :timeout-blocking (let [{:keys [timeout units]} opts]
                        (TimeoutBlockingWaitStrategy. timeout (time-units units)))
    :yielding (YieldingWaitStrategy.)))

(defn ^RingBuffer create-ring-buffer
  [^EventFactory factory size & {:keys [type wait-strategy]
                                 :or {type :multi
                                      wait-strategy :blocking}
                                 :as opts}]
  (case type
    :multi (RingBuffer/createMultiProducer factory
                                           size
                                           (create-strategy wait-strategy opts))
    :single (RingBuffer/createSingleProducer factory
                                             size
                                             (create-strategy wait-strategy opts))))

(defn ^RingBuffer add-gating-sequences
  [^RingBuffer rb & sequences]
  (doto rb
    (.addGatingSequences (into-array Sequence sequences))))

(defn ^RingBuffer remove-gating-sequence
  [^RingBuffer rb ^Sequence sequence]
  (doto rb
    (.removeGatingSequence sequence)))

(defn ^SequenceBarrier sequence-barrier
  [^RingBuffer rb & sequences]
  (.newBarrier rb (into-array Sequence sequences)))

(defn ^BatchEventProcessor set-exception-handler
  [^BatchEventProcessor bep exception-handler]
  (when-let [handler (create-exception-handler (or exception-handler :none))]
    (.setExceptionHandler bep handler))
  bep)

(defn ^BatchEventProcessor create-batch-event-processor
  [^DataProvider rb ^SequenceBarrier barrier ^EventHandler handler & {:keys [exception-handler]}]
  (BatchEventProcessor. rb barrier handler))

(defn ^WorkerPool create-worker-pool
  [^RingBuffer rb ^SequenceBarrier barrier ^ExceptionHandler e-handler & work-handlers]
  (WorkerPool. rb barrier e-handler (into-array WorkHandler work-handlers)))

(defn ^RingBuffer start-worker-pool
  [^WorkerPool wp ^Executor executor]
  (.start wp executor))

(defn get-worker-sequences
  [^WorkerPool wp]
  (.getWorkerSequences wp))

(defn ^Sequence get-sequence
  [^BatchEventProcessor bep]
  (.getSequence bep))

(defn ^SequenceGroup add-to-sequence-group
  [^SequenceGroup group & sequences]
  (doseq [^Sequence sequence sequences]
    (.add group sequence))
  group)

(defn ^SequenceGroup sequence-group
  [& sequences]
  (doto (SequenceGroup.)
    (add-to-sequence-group sequence)))

(defn ^SequenceGroup add-to-running-sequence-group
  [^Cursored rb ^SequenceGroup group & sequences]
  (doseq [^Sequence sequence sequences]
    (.addWhileRunning group rb sequence))
  group)
