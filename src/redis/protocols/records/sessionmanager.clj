(ns redis.protocols.records.sessionmanager
  (:require
   [redis.protocols.sessionmanager :as p]
   [redis.protocols.impl.sessionmanager :as impl]
   [taoensso.timbre :as log]))


;; ---------------------------------------------------------------------------- SessionManager

(defrecord SessionManager [store]
  p/SessionManager
  (expire! [_ id]
    (log/trace ::expire-session id)
    (.delete-session store id))

  (get-or-create! [_ fingerprint]
    (log/trace ::create-session :enter)
    (impl/get-or-create! store fingerprint))

  (add-item! [_ id path v] (impl/add-item! store id path v))
  (delete-item! [_ id path] (impl/delete-item! store id path))
  (get-item! [_ id path] (impl/get-item! store id path)) 
  (update-item! [_ id path f] (impl/update-item! store id path f)))
