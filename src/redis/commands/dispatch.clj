(ns redis.commands.dispatch)

(defmulti command-dispatch :command)