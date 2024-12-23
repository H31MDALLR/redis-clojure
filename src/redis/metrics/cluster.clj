(ns redis.metrics.cluster
  (:require [redis.metrics.state :as state]))

(defn update-cluster-state! [state]
  (state/set-metric! [:cluster :state] state))

(defn update-slots-assigned! [count]
  (state/set-metric! [:cluster :slots_assigned] count))

(defn update-slots-ok! [count]
  (state/set-metric! [:cluster :slots_ok] count))

(defn update-slots-pfail! [count]
  (state/set-metric! [:cluster :slots_pfail] count))

(defn update-slots-fail! [count]
  (state/set-metric! [:cluster :slots_fail] count))

(defn update-known-nodes! [count]
  (state/set-metric! [:cluster :known_nodes] count))

(defn update-size! [size]
  (state/set-metric! [:cluster :size] size))

(defn update-current-epoch! [epoch]
  (state/set-metric! [:cluster :current_epoch] epoch))

(defn get-cluster-metrics []
  (state/get-section :cluster)) 