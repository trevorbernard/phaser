(ns phaser.disruptor-test
  (:require
   [clojure.test :refer :all]
   [phaser.disruptor :refer :all])
  (:import
   [com.lmax.disruptor EventFactory EventHandler]
   [com.lmax.disruptor.dsl.Disruptor]))

(deftest event-factory-test []
  (let [factory (create-event-factory #(java.util.ArrayList.))]
    (is (instance? EventFactory factory))
    (let [obj1 (.newInstance factory)
          obj2 (.newInstance factory)]
      (is (instance? java.util.ArrayList obj1))
      (is (instance? java.util.ArrayList obj2))
      (is (not (identical? obj1 obj2))))))

(deftest event-handler-test []
  (let [data (atom [])
        handler (create-event-handler
                 (fn [event sequence end?]
                   (swap! data conj [event sequence end?])))]
    (is (instance? EventHandler handler))
    (.onEvent handler "a" 1 false)
    (is (= ["a" 1 false] (first @data)))
    (.onEvent handler "b" 2 true)
    (is (= ["b" 2 true] (second @data)))))
