(ns redis.time
  (:require
   [java-time.api :as jt]
   [taoensso.timbre :as log]))


;; ------------------------------------------------------------------------------------------- Expiry


(defn add-duration
  [timestamp duration-ms]
  (log/trace ::add-duration {:t timestamp
                             :duration duration-ms})
  (jt/plus timestamp (jt/millis duration-ms)))

(defn expired?
  "Returns false if the item is not expired.
   Returns true if the given expiration instant occured in the past."
  [expiration-instant]
  (let [now (jt/instant)
        expired? (jt/before? (jt/instant expiration-instant) now)]
    (log/trace ::expired? {:expiry expiration-instant
                           :expired? expired?})
    expired?))

(defn set-expiry-time
  [options]
  (let [{:keys [milliseconds
                seconds
                unix-time-seconds
                unix-time-milliseconds]} options
        _ (log/trace ::set-expiry-time {:options options})
        expiry (cond
                 milliseconds {:expiry (add-duration (jt/instant) (first milliseconds))}
                 seconds {:expiry (add-duration (jt/instant) (* 1000 (first seconds)))}
                 unix-time-milliseconds {:expiry (jt/instant (first unix-time-milliseconds))}
                 unix-time-seconds {:expiry (jt/instant (* 1000 (first unix-time-seconds)))}
                 :else {})]
    expiry))

(defn apply-expiry-rules
  "Applies REDIS expiry precedence based on expiration options and keepttl.
   Will return a map containing the expiry time or remove existing ttl if no options
   were specified."
  [expiry-map keepttl value]
  (cond
    (seq expiry-map) (into {} [expiry-map value])
    keepttl value
    :else (dissoc value :expiry)))

;; ------------------------------------------------------------------------------------------- Public Interface

(defn now
  "Returns an instant for the current time."
  []
  (jt/instant))

(defn update-last-read-access
  [m]
  (assoc m :last-read-time (jt/instant)))

(defn update-last-write-access
  [m]
  (assoc m :last-write-time (jt/instant)))

(defn map->map-with-expiry
  [m {:keys [keepttl] :as options}]
  (log/trace ::map->map-with-expiry {:keepttl keepttl
                                     :options options})
  (->> options
       set-expiry-time
       (apply-expiry-rules keepttl m)))

;; ------------------------------------------------------------------------------------------- REPL

(comment
  (def data {:timestamp   1700741760000
             :duration-ms 60000})
  (def past (jt/instant))
  (jt/after?  (add-duration (jt/instant) 100) (jt/instant))
  (expired? (add-duration (jt/instant) 100))
  (expired? past)
  (jt/after? (jt/instant "2024-11-25T03:45:05.350066352Z") (jt/instant "2024-11-25T03:45:05.264Z"))
  "Leave this here.")
