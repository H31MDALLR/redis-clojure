(ns redis.commands.echo
  (:require
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [taoensso.timbre :as log]))

(defmethod dispatch/command-dispatch :echo
  [{:keys [command-info] :as ctx}]
  (log/trace ::echo command-info)
  (assoc ctx :response (resp2/bulk-string (-> command-info
                                              :defaults 
                                              first))))