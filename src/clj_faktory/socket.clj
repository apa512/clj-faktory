(ns clj-faktory.socket
  (:require [clojure.java.shell :as shell]
            [clojure.string :as string]
            [cheshire.core :as cheshire]
            [clj-sockets.core :as sockets]
            [crypto.random :as random]
            [pool.core :as pool])
  (:import [java.net InetAddress URI]))

(defn- hostname []
  (or (try (.getHostName (InetAddress/getLocalHost))
           (catch Exception _))
      (some-> (shell/sh "hostname")
              :out
              string/trim)))

(defn- worker-info []
  {:wid (random/hex 12)
   :hostname (hostname)
   :v 2})

(defn- read-and-parse-response [socket]
  (let [[resp-type response] (-> (sockets/read-line socket)
                                 ((juxt first rest))
                                 (update 1 (partial apply str)))]
    (case resp-type
      \+ response
      \$ (when-not (= response "-1")
           (cheshire/parse-string (sockets/read-line socket) true)))))

(defn- command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn send-command [socket command]
  (sockets/write-line socket (command-str command))
  (read-and-parse-response socket))

(defn connect [uri worker-info]
  (let [uri (URI. uri)
        host (.getHost uri)
        port (.getPort uri)
        socket (sockets/create-socket host port)]
    (read-and-parse-response socket)
    (send-command socket [:hello worker-info])
    socket))

(defmacro with-conn [[socket-name pool] & body]
  `(let [socket# (pool/borrow ~pool)]
     (try
      (let [~socket-name socket#]
        ~@body)
      (finally (pool/return ~pool socket#)))))

(defn conn-pool [uri]
  (let [worker-info (worker-info)
        conn-pool (pool/get-pool #(connect uri worker-info)
                                 :destroy sockets/close-socket)]
    {:conn-pool conn-pool
     :wid (:wid worker-info)}))
