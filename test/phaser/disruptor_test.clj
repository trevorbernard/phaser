;; Copyright 2013-2014 UserEvents Inc.
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

(ns phaser.disruptor-test
  (:use
   clojure.test
   phaser.disruptor)
  (:import
   [com.lmax.disruptor EventFactory EventHandler EventTranslator
    EventTranslatorOneArg EventTranslatorTwoArg EventTranslatorThreeArg
    EventTranslatorVararg]
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

(deftest event-tranlator-test []
  (let [translator (create-event-translator (fn [event sequence]
                                              (reset! event "hi")))]
    (is (instance? EventTranslator translator))
    (let [event (atom nil)]
      (.translateTo translator event 0)
      (is (= "hi" @event)))))

(deftest event-tranlator-one-test []
  (let [translator (create-event-translator-one-arg
                    (fn [event sequence arg0]
                      (reset! event (str "hi" arg0))))]
    (is (instance? EventTranslatorOneArg translator))
    (let [event (atom nil)]
      (.translateTo translator event 0 "!")
      (is (= "hi!" @event)))))

(deftest event-tranlator-two-test []
  (let [translator (create-event-translator-two-arg
                    (fn [event sequence arg0 arg1]
                      (reset! event (str "hi" arg0 arg1))))]
    (is (instance? EventTranslatorTwoArg translator))
    (let [event (atom nil)]
      (.translateTo translator event 0 "!" "?")
      (is (= "hi!?" @event)))))

(deftest event-tranlator-three-test []
  (let [translator (create-event-translator-three-arg
                    (fn [event sequence arg0 arg1 arg2]
                      (reset! event (str "hi" arg0 arg1 arg2))))]
    (is (instance? EventTranslatorThreeArg translator))
    (let [event (atom nil)]
      (.translateTo translator event 0 "!" "?" "!")
      (is (= "hi!?!" @event)))))

(deftest event-tranlator-var-test []
  (let [translator (create-event-translator-var-arg
                    (fn [event sequence & args]
                      (reset! event (str "hi" (apply str args)))))]
    (is (instance? EventTranslatorVararg translator))
    (let [event (atom nil)]
      (.translateTo translator event 0 (into-array String
                                                   ["a" "b" "c" "d" "e"]))
      (is (= "hiabcde" @event)))))
