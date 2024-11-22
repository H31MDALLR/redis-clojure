(ns redis.commands.ping)

(defn ping [& args]
  "+PONG\r\n")