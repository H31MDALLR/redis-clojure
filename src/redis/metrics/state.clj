(ns redis.metrics.state
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

;; Load initial metrics structure from info.edn
(def initial-metrics
  (-> (io/resource "info.edn")
      slurp
      edn/read-string))

;; Single atom for all metrics
(def metrics-state (atom initial-metrics))

;; Helper functions for updating metrics
(defn update-metric! 
  "Update a metric value in the metrics map. Path is a vector of keys."
  [path f & args]
  (apply swap! metrics-state update-in path f args))

(defn set-metric! 
  "Set a metric value in the metrics map. Path is a vector of keys."
  [path value]
  (swap! metrics-state assoc-in path value))

(defn get-metric 
  "Get a metric value from the metrics map. Path is a vector of keys."
  ([path] (get-metric path nil))
  ([path not-found]
  (get-in @metrics-state path not-found)))

(defn get-section 
  "Get an entire section of metrics"
  [section-key]
  (get @metrics-state section-key))