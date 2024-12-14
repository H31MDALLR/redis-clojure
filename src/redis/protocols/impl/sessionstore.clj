(ns redis.protocols.impl.sessionstore)

;; ---------------------------------------------------------------------------- SessionStore

(defn store-session [{:keys [fingerprint-map sessions] :as record} id session]
  (swap! sessions assoc id session)
  (swap! fingerprint-map assoc (:fingerprint session) id))

(defn retrieve-session [{:keys [sessions] :as record} id]
  (get @sessions id))

(defn delete-session [{:keys [fingerprint-map  sessions] :as record} id]
  (when-let [session (retrieve-session record id)]
    (swap! sessions dissoc id)
    (swap! fingerprint-map dissoc (:fingerprint session))))

(defn get-session-by-fingerprint [{:keys [fingerprint-map] :as record} fingerprint]
  (when-let [id (get @fingerprint-map fingerprint)]
    (retrieve-session record id)))