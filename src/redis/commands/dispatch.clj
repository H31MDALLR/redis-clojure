(ns redis.commands.dispatch 
  (:require
   [taoensso.timbre :as log]
   
   [redis.encoding.resp2 :as resp2]))

(defmulti command-dispatch #(get-in % [:command-info :command]))

(defmethod command-dispatch :default [{:keys [command-info]
                                       :as   ctx}]
  (let [command (:command command-info)]
    (log/error ::command-dispatch {:anomaly      :anomalies/not-found
                                   :command-info command-info})
    (assoc ctx :response (resp2/error (str "Unknown command: " command)))))
