(ns redis.metrics.server
  (:require [redis.metrics.state :as state]))

(defn update-port! [port]
  (state/set-metric! [:server :port] port))

(defn update-config-file! [file]
  (state/set-metric! [:server :config-file] file))

(defn update-io-threads! [threads]
  (state/set-metric! [:server :io-threads] threads))

(defn get-server-metrics []
  (state/get-section :server))
