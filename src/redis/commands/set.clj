(ns redis.commands.set 
  (:require
    [redis.commands.dispatch :as dispatch]
    [redis.encoder :as encoder]))

(defmethod dispatch/command-dispatch :set
  [_]
  (encoder/encode-resp {:simple-string "OK"}))