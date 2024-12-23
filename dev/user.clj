(ns user
  (:require
   [integrant.core :as ig]
   [integrant.repl :as ir]
   [redis.config :as config]))

(defn set-prep! []
  (ir/set-prep! #(ig/expand (config/refresh-configuration) (ig/deprofile [:dev]))))

(defn prep 
  "Prepares the system by setting up the configuration in integrant.repl.state/config."
  [] 
  (ir/prep))

(defn init 
  "Initiate the configuration in integrant.repl.state/config."
  [] (ir/init))

(defn go 
  "Starts the system by calling prep and init."
  []
  (ir/go))

(defn reset 
  "Resets the system by reloading source *only changed* files."
  [] 
  (ir/reset))

(defn reset-all 
  "Resets the system by reloading source *all* files."
  [] 
  (ir/reset-all))

(defn suspend 
  "Suspends the system."
  [] 
  (ir/suspend))

(defn halt 
  "Stops the system."
  [] 
  (ir/halt))

(defn clear 
  "Clears the system."
  [] 
  (ir/clear))

(comment

  (set-prep!)
  (prep)
  (init)

  ;; start system (prep + init)
  (go)

  ;; reset system and reload source *all* files
  (reset)

  ;; reset system and reload source *all* files
  (reset-all)

  ;; suspend system
  (suspend)

  ;; stop system
  (halt)

  ;; clear all state
  (clear)

  ::leave-this-here
  )