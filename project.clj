(defproject userevents/phaser "0.1.0-SNAPSHOT"
  :description "A Clojure DSL for the LMAX Disruptor"
  :url "https://github.com/userevents/phaser"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.googlecode.disruptor/disruptor "2.10.4"]]
  :main phaser.test
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :min-lein-version "2.0.0")
