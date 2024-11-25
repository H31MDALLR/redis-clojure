(ns redis.commands.ping
  (:require [redis.commands.dispatch :as dispatch]
            [redis.encoding.resp2 :as resp2]))

(defmethod dispatch/command-dispatch :ping
  [{:keys [msg]}] 
  (if msg 
    (resp2/encode-resp {:simple-string msg})
    (resp2/encode-resp {:simple-string "PONG"})))