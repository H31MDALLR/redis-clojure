(ns redis.commands.set 
  (:require
    [redis.commands.command :as command]
    [redis.encoder :as encoder]))

(defmethod command/command :set
  [_]
  (encoder/encode-resp {:simple-string "OK"}))