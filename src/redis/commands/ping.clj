(ns redis.commands.ping
  (:require [redis.commands.dispatch :as dispatch]
            [redis.encoding.resp2 :as resp2]))



(defn- impl-ping [{:keys [command-info] :as ctx}]
  (let [defaults (:defaults command-info)]
    (assoc ctx :response
           (if (seq defaults)
             (resp2/encode-resp {:simple-string (first defaults)})
             (resp2/encode-resp {:simple-string "PONG"})))))


(defmethod dispatch/command-dispatch :ping
  [ctx]
  (impl-ping ctx))