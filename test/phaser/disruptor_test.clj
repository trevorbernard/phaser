(ns phaser.disruptor-test
  (:use clojure.test
        phaser.disruptor)
  (:import [com.lmax.disruptor EventHandler EventTranslator]
           [com.lmax.disruptor.dsl Disruptor]))

(deftest event-handler-test []
  (let [world (atom nil)
        handler (event-handler
                 [event sequence end-of-batch?]
                 (reset! world {:event event
                                :sequence sequence
                                :end-of-batch? end-of-batch?}))]
    (is (instance? EventHandler handler))
    (.onEvent handler "hi" 1337 false)
    (let [{:keys [event sequence end-of-batch?]} @world]
      (is (= "hi" event))
      (is (= 1337 sequence))
      (is (not end-of-batch?)))))