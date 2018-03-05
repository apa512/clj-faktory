(ns clj-faktory.core
  (:require [clojure.core.async :as async]
            [aleph.tcp :as tcp]
            [clj-faktory.net :as net]
            [crypto.random :as random]
            [clj-faktory.transit :as transit])
  (:import [java.net InetAddress]))

(defmulti perform :jobtype)

(defn- encode-args [job]
  (-> job
      (update :args (comp vector transit/write))
      (assoc-in [:custom :args-encoding] "transit")))

(defn- decode-args [job]
  (if (= (get-in job [:custom :args-encoding]) "transit")
    (update job :args (comp transit/read first))
    job))

(defn- run-command-loop [{:keys [client wid heartbeat command-chan]}]
  (async/go-loop []
    (let [[[command resp-chan] ch] (async/alts! [command-chan (async/timeout heartbeat)] :priority true)]
      (if (= ch command-chan)
        (when command
          (async/>! resp-chan (net/send-command client command))
          (async/close! resp-chan)
          (recur))
        (do (net/send-command client [:beat {:wid wid}])
            (recur))))))

(defn run-work-loop [{:keys [client concurrency]}])

(defn send-command [worker-manager command]
  (let [resp-chan (async/chan)]
    (async/go (async/>! (:command-chan worker-manager) [command resp-chan])
              (async/<! resp-chan))))

(defn perform-async [worker-manager job]
  (async/<!! (send-command worker-manager [:push (encode-args job)])))

(defn fetch* [worker-manager]
  (async/go (-> (send-command worker-manager (cons :fetch (:queues worker-manager)))
                async/<!
                decode-args)))

(defn fetch [worker-manager]
  (async/<!! (fetch* worker-manager)))

(defn stop [worker-manager]
  (async/close! (:command-chan worker-manager)))

(defn start [worker-manager]
  (run-command-loop worker-manager))

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
