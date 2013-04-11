(defproject userevents/phaser "1.1.1-SNAPSHOT"
  :description "A Clojure DSL for the LMAX Disruptor"
  :url "https://github.com/userevents/phaser"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.lmax/disruptor "3.0.0"]
                 [org.clojure/tools.macro "0.1.2"]]
  :profiles
  {:1.2.1 {:dependencies [[org.clojure/clojure "1.2.1"]]}
   :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}}
  :aliases {"all" ["with-profile" "dev:1.2.1:1.3:1.4"]}
  :min-lein-version "2.0.0"
  :pom-addition [:developers
                 [:developer
                  [:name "Trevor Bernard"]
                  [:organization "UserEvents Inc."]
                  [:organizationUrl "http://userevents.com"]
                  [:email "trevor@userevents.com"]
                  [:timezone "-4"]]
                 [:developer
                  [:name "Josh Comer"]
                  [:organization "UserEvents Inc."]
                  [:organizationUrl "http://userevents.com"]
                  [:email "josh@userevents.com"]
                  [:timezone "-4"]]
                 [:developer
                  [:name "Ian Bishop"]
                  [:organization "UserEvents Inc."]
                  [:organizationUrl "http://userevents.com"]
                  [:email "ian@userevents.com"]
                  [:timezone "-4"]]])
