(ns redis.commands.info
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.clients :as client-metrics]
   [redis.metrics.command-stats :as command-metrics]
   [redis.metrics.cpu :as cpu-metrics]
   [redis.metrics.error :as error-metrics]
   [redis.metrics.latency :as latency-metrics]
   [redis.metrics.memory :as memory-metrics]
   [redis.metrics.persistence :as persistence-metrics]
   [redis.metrics.replication :as replication-metrics]
   [redis.metrics.server :as server-metrics]
   [redis.metrics.stats :as stats-metrics]
   [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------- Layer 0
;; depends on only things outside this namespace

(def info-template
  (-> (io/resource "info.edn")
      slurp
      edn/read-string))

(defn format-info-section [section-data]
  (->> section-data
       (map (fn [[k v]] (format "%s:%s" (name k) v)))
       (str/join "\r\n")))

(defn get-all-metrics []
  {:clients (client-metrics/get-client-metrics)
   :commandstats (command-metrics/get-command-stats)
   :cpu (cpu-metrics/get-cpu-metrics)
   :errorstats (error-metrics/get-error-metrics)
   :latencystats (latency-metrics/get-command-percentiles)
   :memory (memory-metrics/get-memory-info)
   :persistence (persistence-metrics/get-persistence-metrics)
   :replication (replication-metrics/get-replication-metrics)
   :server (server-metrics/get-server-metrics)
   :stats (stats-metrics/get-stats-metrics)})

(defn get-metric [metric]
  (case metric
    :clients (client-metrics/get-client-metrics)
    :commandstats (command-metrics/get-command-stats)
    :cpu (cpu-metrics/get-cpu-metrics)
    :errorstats (error-metrics/get-error-metrics)
    :latencystats (latency-metrics/get-command-percentiles)
    :memory (memory-metrics/get-memory-info)
    :persistence (persistence-metrics/get-persistence-metrics)
    :replication (replication-metrics/get-replication-metrics)
    :server (server-metrics/get-server-metrics)
    :stats (stats-metrics/get-stats-metrics)))

;; ---------------------------------------------------------------------------- Layer 1
;; depends on only things in layer 0

(defn format-info-response [info-data]
  (->> info-data
       (map (fn [[section-name section-data]]
              (str "# " (str/upper-case (name section-name)) "\r\n"
                   (format-info-section section-data))))
       (str/join "\r\n")))

(defn collect-metrics
  ([& metrics]
   (reduce (fn [acc metric]
             (merge acc (get-metric metric)))
           {} metrics))
  ([] (get-all-metrics)))

(defn info-command [{:keys [command-info] :as ctx}]
  (let [sections (map keyword (:args command-info))
        live-metrics (collect-metrics)
        merged-info (merge-with merge info-template live-metrics)
        _ (log/trace ::info-command {:sections sections})
        requested-info (if (seq sections)
                         (select-keys merged-info sections)
                         merged-info)
        response-text (format-info-response requested-info)]
    (assoc ctx :response (resp2/bulk-string response-text))))

;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :info
  [ctx]
  (log/info ::command-dispatch :info ctx)
  (info-command ctx))

;; ------------------------------------------------------------------------------------------- REPL
(comment
  (info-command {:command-info {:command :info, :args '("clients")}})
  (-> (collect-metrics)
      (select-keys ["clients"]))
  ::live-metrics)
