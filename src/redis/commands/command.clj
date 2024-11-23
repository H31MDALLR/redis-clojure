(ns redis.commands.command
  (:require
   [clojure.string :as str]

   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.encoder :as encoder]))

;; ------------------------------------------------------------------------------------------- Command Handling

(defmulti exec-command (fn [args] (-> args first str/lower-case keyword)))
(defmethod exec-command :docs [_]
  (encoder/encode-resp
   {:array [{:array [{:bulk-string "COMMAND DOC"}
                     {:array [{:bulk-string "summary"}
                              {:bulk-string "Command documentation"}
                              {:bulk-string "since"}
                              {:bulk-string "1.0.0"}
                              {:bulk-string "group"}
                              {:bulk-string "string"}
                              {:bulk-string "complexity"}
                              {:bulk-string "O(1)"}
                              {:bulk-string "history"}
                              {:array []}
                              {:bulk-string "arguments"}
                              {:array []}]}]}
            {:array [{:bulk-string "GET"}
                     {:array [{:bulk-string "summary"}
                              {:bulk-string "Gets a value for the given key."}
                              {:bulk-string "since"}
                              {:bulk-string "1.0.0"}
                              {:bulk-string "group"}
                              {:bulk-string "string"}
                              {:bulk-string "complexity"}
                              {:bulk-string "O(1)"}
                              {:bulk-string "history"}
                              {:array []}
                              {:bulk-string "arguments"}
                              {:array []}]}]}
            {:array [{:bulk-string "PING"}
                     {:array [{:bulk-string "summary"}
                              {:bulk-string "Ping the server."}
                              {:bulk-string "since"}
                              {:bulk-string "1.0.0"}
                              {:bulk-string "group"}
                              {:bulk-string "connection"}
                              {:bulk-string "complexity"}
                              {:bulk-string "O(1)"}
                              {:bulk-string "history"}
                              {:array []}
                              {:bulk-string "arguments"}
                              {:array []}]}]}
            {:array [{:bulk-string "SET"}
                     {:array [{:bulk-string "summary"}
                              {:bulk-string "Sets a value for the given key."}
                              {:bulk-string "since"}
                              {:bulk-string "1.0.0"}
                              {:bulk-string "group"}
                              {:bulk-string "string"}
                              {:bulk-string "complexity"}
                              {:bulk-string "O(1)"}
                              {:bulk-string "history"}
                              {:array [{:array [{:bulk-string "2.6.12"}
                                                {:bulk-string "Added the `EX`, `PX`, `NX` and `XX` options."}]}
                                       {:array [{:bulk-string "6.0.0"}
                                                {:bulk-string "Added the `KEEPTTL` option."}]}
                                       {:array [{:bulk-string "6.2.0"}
                                                {:bulk-string "Added the `GET`, `EXAT` and `PXAT` option."}]}]}
                              {:bulk-string "arguments"}
                              {:array []}]}]}]}))




;; ------------------------------------------------------------------------------------------- Dispatch


(defmethod dispatch/command-dispatch :command
  [{:keys [args]}]
  (log/info ::command-dispatch :command {:args args})
  (exec-command args))


;; ------------------------------------------------------------------------------------------- REPL AREA
