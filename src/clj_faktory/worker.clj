(ns clj-faktory.worker
  (:require [cheshire.core :as cheshire]
            [clj-faktory.client :as client]
            [clj-faktory.protocol.transit :as transit]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crypto.random :as random])
  (:import [clojure.lang IDeref]
           [java.net InetAddress]
           [java.util.concurrent Executors ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

(def registered-jobs
  (atom {}))

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

(defn- hostname []
  (or (try (.getHostName (InetAddress/getLocalHost))
           (catch Exception _))
      (some-> (shell/sh "hostname")
              (:out)
              (string/trim))))

(defn- worker-info []
  {:wid (random/hex 12)
   :hostname (hostname)
   :v 2})

(defn- keep-alive [conn wid heartbeat]
  (let [thread-factory (reify ThreadFactory
                         (newThread [_ runnable]
                           (doto (Thread. runnable)
                             (.setDaemon true))))]
    (.scheduleWithFixedDelay (ScheduledThreadPoolExecutor. 1 thread-factory)
                             (fn []
                               (client/beat conn wid))
                             heartbeat
                             heartbeat
                             TimeUnit/MILLISECONDS)))

(defn connect
  ([uri worker-info]
   (.connect (client/connection uri worker-info)))
  ([uri]
   (connect uri (worker-info))))

(defn- run-work-loop [worker]
  (let [conn (connect (get-in worker [::opts :uri]) (::info worker))
        queues (get-in worker [::opts :queues])]
    (loop []
      (let [{:keys [jid] :as job} (decode-transit-args (client/fetch @conn queues))
            [result e] (when job
                         (try
                          (if-let [handler-fn (get @registered-jobs (keyword (:jobtype job)))]
                            (apply handler-fn (:args job))
                            (throw (Exception. "No handler job type")))
                          [:success]
                          (catch InterruptedException e
                            [:stopped e])
                          (catch Throwable e
                            [:failure e])))]
        (case result
          :success (do (client/ack @conn jid)
                       (recur))
          :failure (do (client/fail @conn jid e)
                       (recur))
          :stopped (client/fail @conn jid e)
          (recur))))))

(defn register-job [job-type handler-fn]
  (if (keyword? job-type)
    (swap! registered-jobs assoc job-type handler-fn)
    (throw (Exception. "Job type must be a keyword"))))

(defn perform-async
  ([worker job-type args opts]
   (if (contains? @registered-jobs job-type)
     (let [jid (random/hex 12)
           job (cond-> (merge {:jid jid
                               :jobtype job-type
                               :args args
                               :queue "default"
                               :retry 25
                               :backtrace 10}
                              opts)
                 (transit-args? args) (encode-transit-args))]
       (client/push @(::conn worker) job)
       jid)
     (throw (Exception. "Job type has not been registered"))))
  ([worker job-type args]
   (perform-async worker job-type args {})))

(defn worker [uri opts]
  (let [{:keys [concurrency heartbeat]
         :as   opts} (merge {:concurrency 10
                             :queues      ["default"]
                             :heartbeat   10000} opts)
        info (worker-info)
        conn (connect uri info)
        beat-conn (connect uri info)
        work-pool (Executors/newFixedThreadPool concurrency)]
    (keep-alive @conn (:wid info) heartbeat)
    {::info info
     ::opts (assoc opts :uri uri)
     ::conn conn
     ::beat-conn beat-conn
     ::work-pool work-pool}))

(defn stop [{:keys [::work-pool ::beat-conn ::conn] :as worker}]
  (try
   (when-not (.awaitTermination work-pool 5000 TimeUnit/MILLISECONDS)
     (.shutdownNow work-pool)
     (.awaitTermination work-pool 5000 TimeUnit/MILLISECONDS))
   (catch InterruptedException e
     (log/debug e)
     (.shutdownNow work-pool))
   (finally
    (.close @conn)
    (.close @beat-conn)))
  worker)

(defn start [worker]
  (dotimes [n (get-in worker [::opts :concurrency])]
    (.submit (::work-pool worker) #(run-work-loop worker)))
  (assoc worker ::started? true))
