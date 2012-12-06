(ns phaser.test
  (:require [phaser.disruptor :as dsl])
  (:import [com.lmax.disruptor EventFactory EventHandler EventTranslator
            ClaimStrategy EventPublisher ClaimStrategy RingBuffer
            WaitStrategy]
           [com.lmax.disruptor.dsl Disruptor]
           [java.util.concurrent Executors ExecutorService]
           [phaser.api Message]))

(defn -main []
  ;; 2 phase commit
  ;; claim your slot
  ;; publish that slot
  (let [executor (Executors/newCachedThreadPool)
;;        disruptor (Disruptor. (Message/FACTORY) 1024 executor)
        disruptor (dsl/disruptor (dsl/event-factory (Message.)) 1024 executor)
        ]
    ))
