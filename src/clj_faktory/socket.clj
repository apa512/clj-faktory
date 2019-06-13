(ns clj-faktory.socket
  (:require [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cheshire.core :as cheshire]
            [clj-sockets.core :as sockets]
            [crypto.random :as random]
            [pool.core :as pool])
  (:import [clojure.lang ExceptionInfo]
           [java.net ConnectException InetAddress URI]
           [java.security MessageDigest]))

(defn- retryable? [e]
  (or (= (type e) ConnectException)
      (-> e (ex-data) (:retry?))))

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
      (do (.close socket)
          (throw (ex-info "Unknown socket error" {:type :socket-error
                                                  :retry? true}))))))

(defn- command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn- send-command-with-socket [socket command]
  (log/debug ">>>" (command-str command))
  (sockets/write-line socket (command-str command))
  (read-and-parse-response socket))

(defn send-command-with-pool [conn-pool command]
  (let [socket (pool/borrow conn-pool)]
    (try
     (send-command-with-socket socket command)
     (finally
      (if (.isClosed socket)
        (.invalidateObject conn-pool socket)
        (pool/return conn-pool socket))))))

(defn with-retries* [f]
  (loop [wait-ms [100 1000 10000 30000]]
    (let [result (try
                  (f)
                  (catch Exception e
                    (if (and (seq wait-ms)
                             (retryable? e))
                      (do (Thread/sleep (first wait-ms))
                          (log/debug "Attempting to reconnect"))
                      (throw e))
                    ::retry))]
      (if (= result ::retry)
        (recur (rest wait-ms))
        result))))

(defmacro with-retries [& body]
  `(with-retries* (fn [] ~@body)))

(defn send-command [conn-pool command]
  (with-retries
    (send-command-with-pool conn-pool command)))

(defn- hash-password
  [password salt iterations]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (loop [i iterations
           bs (.getBytes (str password salt))]
      (if (= i 0)
        (.toString (BigInteger. 1 bs) 16)
        (recur (dec i) (.digest digest bs))))))

(defn connect [uri info]
  (with-retries
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
            (send-command-with-socket socket [:hello (assoc info :pwdhash hashed-password)])
            (throw (Exception. "Server requires password, but none has been configured")))
          (send-command-with-socket socket [:hello info])))
      socket)))

(defn- make-pool [uri worker-info]
  (pool/get-pool #(connect uri worker-info)
                 :destroy sockets/close-socket))

(defn conn-pool [uri]
  (let [worker-info (worker-info)]
    {:conn-pool (make-pool uri worker-info)
     :prio-pool (make-pool uri worker-info)
     :wid (:wid worker-info)}))
