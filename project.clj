(defproject redis "0.1.0-SNAPSHOT"
  :description "A barebones implementation of a Redis server"
  :url "http://github.com/codecrafters-io/redis-starter-clojure"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/core.async "1.6.681"]
                 [org.clojure/tools.cli "1.1.230"]

                 ;; command line
                 [org.clojure/tools.cli "1.1.230"]
                 
                 ;; compression
                 [com.ning/compress-lzf "1.1.2"]
                 
                 ;; configuration 
                 [integrant "0.13.1"]
                 [integrant/repl "0.4.0"]

                 ;; exception utils
                 [org.apache.commons/commons-lang3 "3.17.0"]

                 ;; logging
                 [com.taoensso/timbre "6.0.2"]
                 
                 ;; parsing
                 [instaparse "1.5.0"]

                 ;; state management
                 [aero "1.1.6"]
                 [integrant "0.13.1"]

                 ;; streaming i/o
                 [org.clj-commons/byte-streams "0.3.4"]
                 [org.clj-commons/gloss "0.3.6"]

                 ;; time 
                 [clojure.java-time "1.4.3"]

                 ;; web
                 [aleph "0.6.4"]
                 
                 ]

  :main ^:skip-aot redis.core
  :target-path "/tmp/codecrafters-redis-target/%s"
  :clean-targets ^{:protect false} ["/tmp/codecrafters-redis-target"]
  :resource-paths ["resources"]
  :profiles {:development {
                           :dependencies []
                           :source-paths ["bb" "dev" "src"]
                           :repl-options {:init-ns user}
                           }
             :test        {
                           :dependencies [[integrant/repl "0.4.0"]
                                          [com.jakemccrary/lein-test-refresh "0.26.0"]
                                          [com.github.seancorfield/expectations "2.2.214"]
                                          [lein-ancient "1.0.0-RC3"]
                                          [pjstadig/humane-test-output "0.11.0"]]
                           :injections   [(require 'pjstadig.humane-test-output)
                                          (pjstadig.humane-test-output/activate!)]
                           :source-paths ["test"]
                           :repl-options {:init-ns user}}
             :dev         [:development :test]
             :uberjar     {:aot      :all
                           :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})