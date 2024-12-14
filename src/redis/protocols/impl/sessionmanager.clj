(ns redis.protocols.impl.sessionmanager
  (:require
   [taoensso.timbre :as log]

   [redis.time :as time]))

;; ---------------------------------------------------------------------------- State
(def expiry-default 86400)

;; ---------------------------------------------------------------------------- SessionManager
(defn create-new-session! [store fingerprint]
  (let [id (-> (random-uuid) .toString keyword)
        session (merge {:db 0
                        :fingerprint fingerprint
                        :created-at (time/now)}
                       (time/set-expiry-time {:seconds [expiry-default]}))]
    (.store-session store id session)
    id))

(defn get-or-create! [store fingerprint this]
  (if-let [session (.get-session-by-fingerprint store fingerprint)]
    (if (time/expired? (:expiry session))
      (do
        (log/trace ::get-or-create-session {:renewing session})
        (this (:id session))
        (create-new-session! store fingerprint))
      session)
    (create-new-session! store fingerprint)))

(defn add-item! [store id path v]
  (let [session (.retrieve-session store id)
        updated (-> session
                    (assoc-in path v)
                    (time/update-last-write-access))]
    (.store-session store id updated)))

(defn delete-item! [store id path]
  (let [session (.retrieve-session store id)
        updated (update-in session (butlast path) dissoc (last path))]
    (.store-session store id updated)))

(defn get-item! [store id path]
  (get-in (.retrieve-session store id) path))

(defn update-item! [store id path f]
  (let [session (.retrieve-session store id)
        updated (-> session
                    (update-in path f)
                    (time/update-last-write-access))]
    (.store-session store id updated)))

;; ---------------------------------------------------------------------------- SessionManager Impl
