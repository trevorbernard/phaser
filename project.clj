(defproject userevents/phaser "0.1.3-SNAPSHOT"
  :description "A Clojure DSL for the LMAX Disruptor"
  :url "https://github.com/userevents/phaser"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.googlecode.disruptor/disruptor "2.10.4"]]
  :profiles
  {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.5 {:dependencies [[org.clojure/clojure "1.5.0-RC1"]]}}
  :aliases {"all" ["with-profile" "dev:dev,1.3:dev,1.5"]}
  :deploy-repositories [["releases" {:url "https://repo.userevents.com/content/repositories/releases/"
                              :username [:gpg :env/NEXUS_USERNAME]
                              :password [:gpg :env/NEXUS_PASSWORD]}]
                 ["snapshots" {:url "https://repo.userevents.com/content/repositories/snapshots/"
                               :username [:gpg :env/NEXUS_USERNAME]
                               :password [:gpg :env/NEXUS_PASSWORD]
                               :update :always}]]
  :min-lein-version "2.0.0")
