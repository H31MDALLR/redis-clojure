(defproject redis "0.1.0-SNAPSHOT"
  :description "A barebones implementation of a Redis server"
  :url "http://github.com/codecrafters-io/redis-starter-clojure"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/tools.cli "1.1.230"]

                 ;; configuration 
                 [integrant "0.13.1"]
                 
                 ;; exception utils
                 [org.apache.commons/commons-lang3 "3.17.0"]
                 
                 ;; logging
                 [com.taoensso/timbre "6.0.2"]
                 
                 ;; parsing
                 [instaparse "1.5.0"]

                 ;; state management
                 [aero "1.1.6"]
                 [integrant "0.13.1"]

                 ;; time 
                 [clojure.java-time "1.4.3"]

                 ;; web
                 [aleph "0.6.4"]
                 [org.clj-commons/gloss "0.3.6"]
                 
                 ]

  :main ^:skip-aot redis.core
  :target-path "/tmp/codecrafters-redis-target/%s"
  :clean-targets ^{:protect false} ["/tmp/codecrafters-redis-target"]
  :resource-paths ["resources"]
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
