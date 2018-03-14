(ns clj-faktory.core
  (:require [cheshire.core :as cheshire]
            [crypto.random :as random]
            [pool.core :as pool]
            [clj-faktory.protocol.transit :as transit]
            [clj-faktory.socket :as socket]))

(def ^:private registered-jobs (atom {}))

(defn- encode-transit-args [job]
  (-> job
      (update :args (comp vector transit/write))
      (assoc-in [:custom :args-encoding] "transit")))

(defn- decode-transit-args [job]
  (if (= (get-in job [:custom :args-encoding]) "transit")
    (update job :args (comp transit/read first))
    job))

(defn transit-args? [args]
  (not= args (cheshire/parse-string (cheshire/generate-string args))))

(defn- send-command [conn-pool command]
  (try
   (socket/with-conn [conn conn-pool]
     (socket/send-command conn command))
   (catch java.lang.IllegalStateException _
     :closed)))

(defn fail [conn-pool jid e]
  (send-command conn-pool [:fail {:jid jid
                                  :message (.getMessage e)
                                  :errtype (str (class e))
                                  :backtrace (map #(.toString %) (.getStackTrace e))}]))

(defn beat [conn-pool wid]
  (send-command conn-pool [:beat {:wid wid}]))

(defn ack [conn-pool jid]
  (send-command conn-pool [:ack {:jid jid}]))

(defn fetch [conn-pool queues]
  (send-command conn-pool (cons :fetch queues)))

(defn push [conn-pool job]
  (send-command conn-pool [:push job]))

(defn perform-async
  ([{:keys [conn-pool]} job-type args opts]
   (if (contains? @registered-jobs job-type)
     (let [jid (random/hex 12)
           job (cond-> (merge {:jid jid
                               :jobtype job-type
                               :args args
                               :queue "default"
                               :retry 25
                               :backtrace 10}
                              opts)
                 (transit-args? args) encode-transit-args)]
       (push conn-pool job)
       jid)
     (throw (Exception. "Job type has not been registered"))))
  ([worker-manager job-type args]
   (perform-async worker-manager job-type args {})))

(defn info [{:keys [conn-pool]}]
  (send-command conn-pool [:info]))

(defn- run-work-loop [{:keys [conn-pool queues]}]
  (future
   (loop []
     (let [job (decode-transit-args (fetch conn-pool queues))]
       (when-not (= job :closed)
         (when job
           (try
            (let [handler-fn (get @registered-jobs (keyword (:jobtype job)))]
              (apply handler-fn (:args job)))
            (ack conn-pool (:jid job))
            (catch Exception e
              (fail conn-pool (:jid job) e))))
         (recur))))))

(defn stop [{:keys [conn-pool]}]
  (pool/close conn-pool))

(defn start [worker-manager]
  (future
   (loop []
     (Thread/sleep (:heartbeat worker-manager))
     (beat (:conn-pool worker-manager) (:wid worker-manager))
     (recur)))
  (dotimes [_ (:concurrency worker-manager)]
    (run-work-loop worker-manager))
  worker-manager)

(def conn-pool socket/conn-pool)

(defn register-job [job-type handler-fn]
  (if (keyword? job-type)
    (swap! registered-jobs assoc job-type handler-fn)
    (throw (Exception. "Job type must be a keyword"))))

(defn worker-manager
  ([conn-pool {:keys [concurrency
                      heartbeat
                      queues] :or {concurrency 10
                                   heartbeat 15000
                                   queues ["default"]}}]
   (merge conn-pool
          {:concurrency concurrency
           :heartbeat heartbeat
           :queues queues}))
  ([conn-pool]
   (worker-manager conn-pool {})))
