(ns redis.time
  (:require
   [java-time.api :as jt]))


;; ------------------------------------------------------------------------------------------- Expiry

(defn add-duration
  [timestamp duration-ms]
  (jt/plus timestamp (jt/millis duration-ms)))

(defn expired?
  "Returns true if the given expiration instant occured in the past."
  [expiration-instant]
  (if (jt/before? (jt/instant expiration-instant) (jt/instant))
    :expired
    :active))

(defn set-expiry-time
  [options]
  (let [{:keys [milliseconds
                seconds
                unix-time-seconds
                unix-time-milliseconds]} options
        expiry (cond
                 milliseconds {:expiry (add-duration (jt/instant) milliseconds)}
                 seconds {:expiry (add-duration (jt/instant) (* 1000 seconds))}
                 unix-time-milliseconds {:expiry (jt/instant unix-time-milliseconds)}
                 unix-time-seconds {:expiry (jt/instant (* 1000 unix-time-seconds))}
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

(defn update-write-access-time
  [m]
  (assoc m :last-write (jt/instant)))

(defn map->map-with-expiry
  [m {:keys [keepttl] :as options}]
  (->> options
       set-expiry-time
       (apply-expiry-rules keepttl m)))

;; ------------------------------------------------------------------------------------------- REPL

(comment

  (def data {:timestamp   1700741760000
             :duration-ms 60000})

  "Leave this here.")
