(ns clj-faktory.net
  (:require [clojure.string :as string]
            [aleph.tcp :as tcp]
            [byte-streams]
            [cheshire.core :as cheshire]
            [crypto.random :as random]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.stream :as s]
            [clj-faktory.protocol.redis :as redis])
  (:import [java.net InetAddress]))

(def ^:private response-codec
  (redis/redis-codec :utf-8))

(gloss/defcodec- command-codec
  (gloss/string :utf-8 :delimiters ["\r\n"]))

(defn- worker-info []
  {:wid (random/hex 12)
   :hostname (.getHostName (InetAddress/getLocalHost))
   :v 2})

(defn- command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn- read-response [client]
  (io/decode response-codec @(s/take! client)))

(defn- read-and-parse-response [client]
  (let [[resp-type response] (read-response client)]
    (case resp-type
      :single-line response
      :bulk (cheshire/parse-string response true)
      :error (throw (Exception. response)))))

(defn- send-command* [client command]
  @(s/put! client (io/encode command-codec (command-str command))))

(defn send-command [client command]
  (send-command* client command)
  (read-and-parse-response client))

(defn handshake [client]
  (let [worker-info (worker-info)]
    (read-and-parse-response client)
    (send-command client [:hello worker-info])
    worker-info))

(defn connect [url]
  @(tcp/client {:host "localhost"
                :port 7419}))
