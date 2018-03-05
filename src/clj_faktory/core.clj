(ns clj-faktory.core
  (:require [clojure.core.async :as async]
            [aleph.tcp :as tcp]
            [clj-faktory.net :as net]
            [crypto.random :as random])
  (:import [java.net InetAddress]))

(defn run-command-loop [{:keys [client wid heartbeat command-chan]}]
  (async/go-loop []
    (let [[[command resp-chan] ch] (async/alts! [command-chan (async/timeout heartbeat)] :priority true)]
      (if (= ch command-chan)
        (when command
          (async/>! resp-chan (net/send-command client command))
          (recur))
        (do (net/send-command client [:beat {:wid wid}])
            (recur))))))

;(defmulti perform :jobtype)
;
;(defn push [{:keys [client]} job-type args & {:keys [queue] :or {queue "default"}}]
;  (let [job {:jid (random/hex 12)
;             :jobtype job-type
;             :args args
;             :queue queue}]
;    (net/push client job)))

(defn start [worker-manager]
  (run-command-loop worker-manager))

(defn stop [worker-manager]
  (async/close! (:command-chan worker-manager)))

(defn send-command [worker-manager command]
  (let [resp-chan (async/chan)]
    (async/go (async/>! (:command-chan worker-manager) [command resp-chan])
              (async/<! resp-chan))))

(defn perform-async [worker-manager job]
  (async/<!! (send-command worker-manager [:push job])))

(defn fetch [worker-manager]
  (async/<!! (send-command worker-manager (cons :fetch (:queues worker-manager)))))

(defn worker-manager
  ([{:keys [concurrency heartbeat queues] :or {concurrency 10
                                               heartbeat 15000
                                               queues ["default"]}}]
   (let [client (net/connect nil)
         {:keys [wid]} (net/handshake client)]
     {:client client
      :wid wid
      :concurrency concurrency
      :heartbeat heartbeat
      :queues queues
      :command-chan (async/chan 1)}))
  ([]
   (worker-manager {})))
