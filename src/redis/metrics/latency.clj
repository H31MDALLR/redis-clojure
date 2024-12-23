(ns redis.metrics.latency
  (:require [redis.metrics.state :as state]))

;; ---------------------------------------------------------------------------- Defs

(def p50-idx 499)
(def p99-idx 989)
(def p999-idx 998)

;; ---------------------------------------------------------------------------- Layer 0
;; only depends on things outside of this namespace.

(defn calculate-percentile [samples idx]
  (when (seq samples)
    (nth (vec samples) idx 0)))


(defn update-samples [samples new-value]
  (let [samples (or samples (sorted-set))
        samples (conj samples new-value)]
    (if (> (count samples) 1000)
      (disj samples (last samples))
      samples)))

;; ---------------------------------------------------------------------------- Layer 1
;; only depends on things in layer 0

(defn record-command-latency! [command-type duration-usec]
  (state/update-metric! [:latencystats (str "latency_percentile_" (name command-type)) :samples]
                       update-samples
                       duration-usec))


(defn get-command-percentiles []
  (->> (state/get-section :latencystats)
       (map (fn [[cmd stats]]
             (let [samples (:samples stats)]
               [cmd {:p50  (calculate-percentile samples p50-idx)
                    :p99  (calculate-percentile samples p99-idx)
                    :p999 (calculate-percentile samples p999-idx)}])))
       (into {})))

;; ---------------------------------------------------------------------------- REPL
(comment 
  (record-command-latency! :get 100)
  (record-command-latency! :set 200)
  (record-command-latency! :incr 300)
  (get-command-percentiles)
  
  ::leave-this-here)