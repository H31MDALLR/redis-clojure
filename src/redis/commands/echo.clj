(ns redis.commands.echo
  (:require
   [taoensso.timbre :as log]
   
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]))

;; ---------------------------------------------------------------------------- command

(defn impl-echo [{:keys [command-info] :as ctx}]
  (log/trace ::echo command-info)
  (assoc ctx :response (resp2/bulk-string (-> command-info
                                              :defaults 
                                              first))))


(defmethod dispatch/command-dispatch :echo
  [ctx]
  (impl-echo ctx))