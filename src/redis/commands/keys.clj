(ns redis.commands.keys
  (:require
   [taoensso.timbre :as log]
   
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.session :as session]
   [redis.storage :as storage]))

;; ---------------------------------------------------------------------------- Layer 0
;; only depends on things outside this file.

(defn keys-cmd [command-info session-id]
  (let [{:keys [defaults]} command-info
        db (.get-item! session/sm session-id [:db])
        matches (storage/find-keys db (first defaults))]
    (log/trace ::keys-cmd {:matches matches})
    matches))

;; ---------------------------------------------------------------------------- Layer 1
;; depends on layer 1 only

;; -------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :keys
  [{:keys [command-info session-id] :as ctx}]
  (log/trace ::command-dispatch :keys command-info)
  (let [matches (keys-cmd command-info session-id)]
    (assoc ctx :response (resp2/coll->resp2-array matches))))


;; ----------------------------------------------------------------------------REPL AREA
(comment 
  (keys-cmd {:command :keys, :defaults ["*"], :options {}} :c327410e-763a-42b0-8af7-f228cad0cfb6)
  ::leave-this-here
  )
