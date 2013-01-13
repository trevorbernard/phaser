(defproject userevents/phaser "0.1.0"
  :description "A Clojure DSL for the LMAX Disruptor"
  :url "https://github.com/userevents/phaser"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.googlecode.disruptor/disruptor "2.10.4"]]
  :profiles
  {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}}
  :aliases {"all" ["with-profile" "dev:dev,1.3:dev,1.5"]}
  :min-lein-version "2.0.0")
