(ns redis.protocols.impl.sessionstore
  (:require [redis.metrics.memory :as memory]))

;; Helper functions for memory tracking
(defn- estimate-session-size [session]
  (let [session-str (pr-str session)]
    (* 2 (count session-str))))  ; UTF-16 chars

(defn- track-session-memory! [session]
  (let [size (estimate-session-size session)]
    (memory/track-memory-usage! size)))

(defn- untrack-session-memory! [session]
  (let [size (estimate-session-size session)]
    (memory/track-memory-usage! (- size))))

;; ---------------------------------------------------------------------------- SessionStore

(defn store-session [{:keys [fingerprint-map sessions] :as record} id session]
  (let [old-session (get @sessions id)]
    (when old-session
      (untrack-session-memory! old-session))
    (track-session-memory! session)
    (swap! sessions assoc id session)
    (swap! fingerprint-map assoc (:fingerprint session) id)))

(defn retrieve-session [{:keys [sessions] :as record} id]
  (get @sessions id))

(defn delete-session [{:keys [fingerprint-map sessions] :as record} id]
  (when-let [session (retrieve-session record id)]
    (untrack-session-memory! session)
    (swap! sessions dissoc id)
    (swap! fingerprint-map dissoc (:fingerprint session))))

(defn get-session-by-fingerprint [{:keys [fingerprint-map] :as record} fingerprint]
  (when-let [id (get @fingerprint-map fingerprint)]
    (retrieve-session record id)))