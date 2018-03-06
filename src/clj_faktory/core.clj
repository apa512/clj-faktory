(ns clj-faktory.core
  (:require [clojure.core.async :as async]
            [aleph.tcp :as tcp]
            [clj-faktory.net :as net]
            [crypto.random :as random]
            [clj-faktory.protocol.transit :as transit]
            [manifold.stream :as stream]))

(defmulti perform (comp keyword :jobtype))

(defmethod perform :default [job]
  (throw (Exception. (str "No handler for job type " (keyword (:jobtype job))))))

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
          (let [resp (net/send-command client command)]
            (when resp
              (async/>! resp-chan resp))
            (async/close! resp-chan)
            (recur)))
        (do (net/send-command client [:beat {:wid wid}])
            (recur))))))

(defn send-command [worker-manager command]
  (let [resp-chan (async/chan)]
    (async/go (if (async/>! (:command-chan worker-manager) [command resp-chan])
                (async/<! resp-chan)
                (do (async/close! resp-chan)
                    :stop)))))

(defn perform-async [worker-manager job]
  (async/<!! (send-command worker-manager [:push (encode-args job)])))

(defn fetch* [worker-manager]
  (async/go
   (let [chan (send-command worker-manager (cons :fetch (:queues worker-manager)))
        resp (async/<! chan)]
    (decode-args resp))))

(defn fetch [worker-manager]
  (async/<!! (fetch* worker-manager)))

(defn handle-job [worker-manager {:keys [jid] :as job}]
  (try (perform job)
       (send-command worker-manager [:ack {:jid jid}])
       (catch Exception e
         (send-command worker-manager [:fail {:jid jid
                                              :errtype "Exception"
                                              :message (.getMessage e)}]))))

(defn run-work-loop [{:keys [client concurrency] :as worker-manager}]
  (dotimes [_ concurrency]
    (future (loop []
              (let [job (fetch worker-manager)]
                (when-not (= job :stop)
                  (when job
                    (handle-job worker-manager job))
                  (recur)))))))

(defn stop [{:keys [command-chan client]}]
  (when command-chan
    (async/close! command-chan))
  #_(stream/close! client))

(defn start [worker-manager]
  (run-command-loop worker-manager)
  (run-work-loop worker-manager)
  worker-manager)

(defn connect
  ([uri]
   (let [client (net/connect uri)
         {:keys [wid]} (net/handshake client)]
     {:client client
      :wid wid}))
  ([]
   (connect "tcp://localhost:7419")))

(defn worker-manager
  ([client {:keys [concurrency heartbeat queues] :or {concurrency 1
                                                      heartbeat 15000
                                                      queues ["default"]}}]
   (-> {:concurrency concurrency
        :heartbeat heartbeat
        :queues queues
        :command-chan (async/chan 1)}
       (merge client)
       start))
  ([client]
   (worker-manager client {})))
