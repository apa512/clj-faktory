(ns clj-faktory.client
  (:require [cheshire.core :as cheshire]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clj-sockets.core :as sockets])
  (:import [clojure.lang IDeref]
           [java.net InetAddress URI]
           [java.security MessageDigest]))

(defprotocol Connectable
  (connect [this])
  (reconnect [this]))

(defn- read-and-parse-response [conn]
  (let [response (sockets/read-line conn)
        _ (log/debug "<<<" response)
        [resp-type message] (-> response
                                ((juxt first rest))
                                (update 1 (partial apply str)))]
    (case resp-type
      \+ (some-> message
                 (string/split #" ")
                 (second)
                 (cheshire/parse-string true))
      \$ (when-not (= message "-1")
           (cheshire/parse-string (sockets/read-line conn) true))
      \- (throw (Exception. message))
      (throw (ex-info "Unknown conn error" {:type ::conn-error})))))

(defn- command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn- send-command* [socket command]
  (log/debug ">>>" (command-str command))
  (sockets/write-line socket (command-str command))
  (read-and-parse-response socket))

(defn- send-command [socket command]
  (loop [retry-ms [1000 10000 30000]]
    (let [[status result] (try
                           [:success (send-command* socket command)]
                           (catch Exception e
                             (if (and (seq retry-ms)
                                      (= (:type (ex-data e)) ::conn-error))
                               [:failure]
                               (do (log/warn e)
                                   (throw e)))))]
      (case status
        :success result
        :failure (let [wait-ms (first retry-ms)]
                   (log/warn (str "Connection error. Retrying in " wait-ms " ms."))
                   (Thread/sleep wait-ms)
                   (recur (rest retry-ms)))))))

(defn- hash-password
  [password salt iterations]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (loop [i iterations
           bs (.getBytes (str password salt))]
      (if (= i 0)
        (.toString (BigInteger. 1 bs) 16)
        (recur (dec i)
               (.digest digest bs))))))

(defn fail [conn jid e]
  (send-command conn [:fail {:jid jid
                             :message (.getMessage e)
                             :errtype (str (class e))
                             :backtrace (map #(.toString %) (.getStackTrace e))}]))

(defn ack [conn jid]
  (send-command conn [:ack {:jid jid}]))

(defn fetch [conn queues]
  (send-command conn (cons :fetch queues)))

(defn beat [conn wid]
  (send-command conn [:beat {:wid wid}]))

(defn push [conn job]
  (send-command conn [:push job]))

(defn- connect* [uri worker-info]
  (let [uri (URI. uri)
        host (.getHost uri)
        port (.getPort uri)
        conn (sockets/create-socket host port)]
    (.setKeepAlive conn true)
    (let [{version :v
           salt :s
           iterations :i} (read-and-parse-response conn)]
      (if salt
        (if-let [hashed-password (some-> (.getUserInfo uri)
                                         (string/split #":")
                                         (last)
                                         (hash-password salt iterations))]
          (send-command conn [:hello (assoc worker-info :pwdhash hashed-password)])
          (throw (Exception. "Server requires password, but none has been configured")))
        (send-command conn [:hello worker-info])))
    conn))

(deftype Connection [uri worker-info conn-atom]
  IDeref
  (deref [this]
    @conn-atom)
  Connectable
  (connect [this]
    (reset! conn-atom (connect* uri worker-info))
    this)
  (reconnect [this]
    (when-let [conn @conn-atom]
      (.close conn))
    (connect this)))

(defn connection [uri worker-info]
  (Connection. uri worker-info (atom nil)))
