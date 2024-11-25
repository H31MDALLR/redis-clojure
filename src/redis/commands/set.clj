(ns redis.commands.set
  (:require
   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.storage :as storage]
   [redis.time :as time]))

;; ------------------------------------------------------------------------------------------- Data

(def expiry-keys #{:milliseconds
                   :seconds
                   :unix-time-seconds
                   :unix-time-milliseconds})

;; ------------------------------------------------------------------------------------------- Util fns
(defn mutually-exclusive-options [nx xx options]
  (let [ expiry-keys (select-keys options expiry-keys)]
    (cond
                               ;; defensive - note - parsing is broken if these get through.
      (and nx xx)
      (do (log/error "NX and XX options returned from parsing. Parsing is broken!")
          (resp2/error "NX and XX options are mutually exclusive"))
      
      (> (count expiry-keys) 1)
      (do (log/error "Mutually exlusive expiration options made it through parsing!" (filter some? expiry-keys))
          (resp2/error (str "Expiration options are mutually exclusive, given: " (filter some? expiry-keys)))))))

(defn- store-and-get [k value nx xx]
  (let [get-result          (storage/retrieve k)]
    (storage/store k value)
    (cond
      (and nx (empty? get-result)) (resp2/bulk-string get-result)
      (and nx (seq get-result)) (resp2/bulk-string nil)
      (and xx (empty? get-result)) (resp2/bulk-string nil)
      (and xx (seq get-result)) (resp2/bulk-string get-result))))


;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :set
  [{:keys [defaults options]}]
  (let [[k v]               defaults
        {:keys [get nx xx]} options 

        ;; grab it before write ops if needed
        get-result          (when get (resp2/bulk-string (storage/retrieve k)))
        option-errors       (mutually-exclusive-options nx xx options)]
    (log/trace ::dispatch :set {:defaults defaults
                                :options  options})

    (if option-errors
      option-errors

      ;; Expiration, NX and XX options parsing 
      (let [expiry         (time/map->map-with-expiry v options)
            value          (into {:value v} expiry)
            encoded-result (if (or nx xx) 
                             (store-and-get k value nx xx)
                             (do (storage/store k value)
                                 (resp2/ok)))]
        
        (log/trace ::set {:encoded-result encoded-result})
        
        ;; GET option
        (if get 
          get-result
          encoded-result)))))


;; ------------------------------------------------------------------------------------------- REPL AREA
(comment
  
  "Leave this here.")