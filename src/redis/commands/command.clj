(ns redis.commands.command 
  (:require
   [clojure.string :as str]
   
   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]))

;; ------------------------------------------------------------------------------------------- Command Handling

(defmulti exec-command (fn [args] (-> args first str/lower-case keyword)))
(defmethod exec-command :docs [_] 
  (encoder/encode-resp {:array [{:simple-string "COMMAND DOC"}
                                {:simple-string "PING"}
                                {:simple-string "SET"}]}))



;; ------------------------------------------------------------------------------------------- Dispatch


(defmethod dispatch/command-dispatch :command
  [{:keys [args]}]
  (log/info ::command-dispatch :command {:args args})
  (exec-command args))


;; ------------------------------------------------------------------------------------------- REPL AREA
