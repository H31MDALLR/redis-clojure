(ns redis.commands.ping
  (:require [redis.commands.command :as command]
            [redis.encoder :as encoder]))

(defmethod command/command :ping
  [{:keys [msg]}] 
  (if msg 
    (encoder/encode-resp {:simple-string msg})
    (encoder/encode-resp {:simple-string "PONG"})))