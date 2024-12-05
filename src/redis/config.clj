(ns redis.config 
  (:require
   [aero.core :as aero]
   [clojure.java.io :as io]
   [integrant.core :as ig]
   [taoensso.timbre :as log]))

(def config (atom {}))

(def env (atom nil))
(defn set-env [environment]
  (log/trace ::set-env {:env environment})
  (reset! env environment))

(defmethod aero.core/reader 'ig/ref
  [{:keys [profile]
    :or {profile "localhost"}
    :as   opts} tag value]
  (ig/ref value))

(defn load-config [resource]
  (try
    (log/trace ::load-config {:profile @env})
    (aero/read-config resource {:profile @env})
    (catch java.io.FileNotFoundException e
      (log/warn :config-file/load {:anomalies/not-found (.getMessage e)})
      {})
    (catch Exception e
      (log/warn :config-file/load {:anomalies/fault (.getMessage e)})
      {})))

(defn merge-all
  [& configs]
  (log/info :config.file/merge-configs configs)
  (let [configurations (map #(load-config %) configs)]
    (apply merge configurations)))

(defn get-configuration []
  (if (seq @config)
    @config
    (reset! config (merge-all (io/resource "config.edn")
                              (io/resource "local_config.edn")))))

(defn refresh-configuration [] 
  (reset! config nil)
  (get-configuration))

(defn get-value [path] 
  (log/trace ::get-value path)
  (get-in (get-configuration) path))

(defn write [path v]
  (swap! config assoc-in path v))

;; -------------------------------------------------------- REPL

(comment 
  (reset! config nil)
  (get-configuration)

  (get-value [:redis/config "dir"])

  ::leave-this-here)