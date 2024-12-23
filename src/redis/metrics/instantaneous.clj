(ns redis.metrics.instantaneous
  (:require [redis.metrics.state :as state]))

(def ^:private tracking-window-ms 1000)

(defn- calculate-rate [current-value last-value elapsed-ms]
  (if (and current-value last-value (pos? elapsed-ms))
    (float (/ (* (- current-value last-value) 1000) elapsed-ms))
    0.0))

(defn update-instantaneous-metrics! []
  (let [now (System/currentTimeMillis)
        last-time (or (state/get-metric [:instantaneous :last_update]) now)
        elapsed (- now last-time)

        ;; Get current values
        commands (state/get-metric [:stats :total_commands_processed])
        input-bytes (state/get-metric [:stats :total_net_input_bytes])
        output-bytes (state/get-metric [:stats :total_net_output_bytes])

        ;; Get last values
        last-commands (state/get-metric [:instantaneous :last_commands])
        last-input (state/get-metric [:instantaneous :last_input])
        last-output (state/get-metric [:instantaneous :last_output])]

    ;; Calculate rates
    (when (>= elapsed tracking-window-ms)
      (state/set-metric! [:stats :instantaneous_ops_per_sec]
                         (calculate-rate commands last-commands elapsed))
      (state/set-metric! [:stats :instantaneous_input_kbps]
                         (/ (calculate-rate input-bytes last-input elapsed) 1024))
      (state/set-metric! [:stats :instantaneous_output_kbps]
                         (/ (calculate-rate output-bytes last-output elapsed) 1024))

      ;; Store current values for next calculation
      (state/set-metric! [:instantaneous]
                         {:last_update now
                          :last_commands commands
                          :last_input input-bytes
                          :last_output output-bytes}))))

;; Start the instantaneous metrics tracking
(defn start-instantaneous-tracking! []
  (future
    (while true
      (try
        (update-instantaneous-metrics!)
        (Thread/sleep tracking-window-ms)
        (catch Exception e
          (println "Error in instantaneous metrics tracking:" (.getMessage e))))))) 