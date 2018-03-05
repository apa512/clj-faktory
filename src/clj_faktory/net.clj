(ns clj-faktory.net
  (:require [clojure.string :as string]
            [aleph.tcp :as tcp]
            [byte-streams]
            [cheshire.core :as cheshire]
            [crypto.random :as random]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.stream :as s])
  (:import [java.net InetAddress]))

(gloss/defcodec command-protocol
  (gloss/string :utf-8 :delimiters ["\r\n"]))

(defn worker-info []
  {:wid (random/hex 12)
   :hostname (.getHostName (InetAddress/getLocalHost))
   :v 2})

(defn command-str [[verb & segments]]
  (->> segments
       (cons (string/upper-case (name verb)))
       (map #(if (map? %)
               (cheshire/generate-string %)
               %))
       (string/join " ")))

(defn read-response [client]
  (-> @(s/take! client)
      byte-streams/to-string
      string/trim-newline))

(defn read-and-parse-response [client]
  (let [[resp-char & response*] (read-response client)
        response (apply str response*)]
    (case resp-char
      \+ response
      \- (throw (Exception. response))
      \$ (-> response
             (string/split #"\r\n")
             last
             (cheshire/parse-string true)))))

(defn ^:private send-command* [client command]
  @(s/put! client (io/encode command-protocol (command-str command))))

(defn send-command [client command]
  (send-command* client command)
  (read-and-parse-response client))

;(defn push [client job]
;  (send-command client "PUSH" job)
;  (read-and-parse-response client))
;
;(defn fetch [client queues]
;  (apply send-command (concat [client "FETCH"] queues))
;  (read-and-parse-response client))
;
;(defn beat [client wid]
;  (send-command client "BEAT" {:wid wid})
;  (read-and-parse-response client))
;
;(defn ack [client jid]
;  (send-command client "ACK" {:jid jid})
;  (read-and-parse-response client))
;
;(defn fail [client jid]
;  (send-command client "FAIL" {:jid jid})
;  (read-and-parse-response client))
;
;(defn info [client]
;  (send-command client "INFO")
;  (read-and-parse-response client))

(defn handshake [client]
  (let [worker-info (worker-info)]
    (read-and-parse-response client)
    (send-command client [:hello worker-info])
    worker-info))

(defn connect [url]
  @(tcp/client {:host "localhost"
                :port 7419}))
