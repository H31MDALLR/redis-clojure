(ns redis.metrics.command-stats
  (:require [redis.metrics.state :as state]))

(defn record-command! [command start-time success?]
  (let [duration (- (System/nanoTime) start-time)
        duration-usec (/ duration 1000)
        cmd-path [:commandstats (str "cmdstat_" (name command))]]
    (state/update-metric! cmd-path
                         (fn [stats]
                           (merge-with +
                                     (or stats
                                         {:calls 0
                                          :usec 0
                                          :rejected_calls 0
                                          :failed_calls 0})
                                     {:calls 1
                                      :usec duration-usec
                                      (if success?
                                        :failed_calls
                                        :rejected_calls) 1})))))

(defn get-command-stats []
  (state/get-section :commandstats))