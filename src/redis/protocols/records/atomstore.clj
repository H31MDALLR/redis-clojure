(ns redis.protocols.records.atomstore 
  (:require
   [redis.protocols.sessionstore :as p]
   [redis.protocols.impl.sessionstore :as impl]))

(defrecord AtomStore [sessions fingerprint-map]
  p/SessionStore
  (store-session [this id session]
    (impl/store-session this id session))

  (retrieve-session [this id]
    (impl/retrieve-session this id))

  (delete-session [this id]
    (impl/delete-session this id))

  (get-session-by-fingerprint [this fingerprint]
    (impl/get-session-by-fingerprint this fingerprint)))