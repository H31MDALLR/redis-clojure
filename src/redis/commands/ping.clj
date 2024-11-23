(ns redis.commands.ping
  (:require [redis.commands.dispatch :as dispatch]
            [redis.encoder :as encoder]))

(defmethod dispatch/command-dispatch :ping
  [{:keys [msg]}] 
  (if msg 
    (encoder/encode-resp {:simple-string msg})
    (encoder/encode-resp {:simple-string "PONG"})))