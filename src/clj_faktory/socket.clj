(ns clj-faktory.socket
  (:require [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cheshire.core :as cheshire]
            [clj-sockets.core :as sockets]
            [crypto.random :as random]
            [pool.core :as pool])
  (:import [java.net InetAddress URI]
           [java.security MessageDigest]))

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
    (log/debug "<<<" resp-type response)
    (case resp-type
      \+ (some-> response
                 (string/split #" ")
                 second
                 (cheshire/parse-string true))
      \$ (when-not (= response "-1")
           (cheshire/parse-string (sockets/read-line socket) true))
      \- (throw (Exception. response))
      (throw (Exception. "Unknown connection error")))))

(defn- command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn send-command [socket command]
  (log/debug ">>>" (command-str command))
  (sockets/write-line socket (command-str command))
  (read-and-parse-response socket))

(defn- hash-password
  [password salt iterations]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (loop [i iterations
           bs (.getBytes (str password salt))]
      (if (= i 0)
        (.toString (BigInteger. 1 bs) 16)
        (recur (dec i) (.digest digest bs))))))

(defn connect [uri info]
  (let [uri (URI. uri)
        host (.getHost uri)
        port (.getPort uri)
        socket (sockets/create-socket host port)]
    (let [{version :v
           salt :s
           iterations :i} (read-and-parse-response socket)]
      (if salt
        (if-let [hashed-password (some-> (.getUserInfo uri)
                                         (string/split #":")
                                         last
                                         (hash-password salt iterations))]
          (send-command socket [:hello (assoc info :pwdhash hashed-password)])
          (throw (Exception. "Server requires password, but none has been configured")))
        (send-command socket [:hello info])))
    socket))

(defmacro with-conn [[socket-name pool] & body]
  `(let [socket# (pool/borrow ~pool)]
     (try
      (let [~socket-name socket#]
        ~@body)
      (finally (pool/return ~pool socket#)))))

(defn- make-pool [uri worker-info]
  (pool/get-pool #(connect uri worker-info)
                 :destroy sockets/close-socket))

(defn conn-pool [uri]
  (let [worker-info (worker-info)]
    {:conn-pool (make-pool uri worker-info)
     :prio-pool (make-pool uri worker-info)
     :wid (:wid worker-info)}))
