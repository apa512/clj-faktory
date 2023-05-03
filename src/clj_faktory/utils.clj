(ns clj-faktory.utils
  (:require [cheshire.core :as cheshire]
            [clj-faktory.protocol.transit :as transit]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [crypto.random :as random])
  (:import [java.net InetAddress]))

(defn encode-transit-args [job]
  (-> job
      (update :args (comp vector transit/write))
      (assoc-in [:custom :args-encoding] "transit")))

(defn transit-args? [args]
  (not= args (cheshire/parse-string (cheshire/generate-string args))))

(defn hostname []
  (or (try (.getHostName (InetAddress/getLocalHost))
           (catch Exception _))
      (some-> (shell/sh "hostname")
              (:out)
              (string/trim))))

(defn client-info []
  {:wid (random/hex 12)
   :hostname (hostname)
   :v 2})