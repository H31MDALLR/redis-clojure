(ns redis.protocols.sessionstore)

;; Protocols
(defprotocol SessionStore
  (store-session [this id session])
  (retrieve-session [this id])
  (delete-session [this id])
  (get-session-by-fingerprint [this fingerprint]))