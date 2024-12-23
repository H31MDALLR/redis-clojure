(ns redis.snowflake)

;; Add these constants for the Snowflake algorithm
(def ^:private epoch 1288834974657) ;; Twitter's epoch (Nov 04 2010 01:42:54.657)
(def ^:private node-id (rand-int 1024)) ;; Random node ID (10 bits)
(def ^:private sequence-bits 12)
(def ^:private node-bits 10)
(def ^:private max-sequence (dec (bit-shift-left 1 sequence-bits)))
(def ^:private sequence-number (atom 0))

(defn generate-snowflake
  "Generates a unique 40-character ID based on Twitter's Snowflake algorithm.
   Returns a string representation of the ID."
  []
  (let [timestamp (- (System/currentTimeMillis) epoch)
        curr-sequence (swap! sequence-number #(if (> % max-sequence) 0 (inc %)))
        id (bit-or
            (bit-shift-left timestamp (+ node-bits sequence-bits))
            (bit-shift-left node-id sequence-bits)
            curr-sequence)]
    (format "%040x" id)))


;; ---------------------------------------------------------------------------- REPL
(comment
  (not= (generate-snowflake) (generate-snowflake))
  ::leave-this-here
  )