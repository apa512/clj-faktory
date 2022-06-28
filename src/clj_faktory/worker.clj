(ns clj-faktory.worker
  (:require [cheshire.core :as cheshire]
            [clj-faktory.client :as client]
            [clj-faktory.protocol.transit :as transit]
            [clojure.java.shell :as shell]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crypto.random :as random])
  (:import [java.net InetAddress]
           [java.util.concurrent Executors ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

(def registered-jobs
  (atom {}))

(def daemon-thread-factory
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. runnable)
        (.setDaemon true)))))

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

(defn- keep-alive [conn pool wid heartbeat]
  (.scheduleWithFixedDelay pool
                           (fn []
                             (client/beat conn wid))
                           heartbeat
                           heartbeat
                           TimeUnit/MILLISECONDS))

(defn connect
  ([uri worker-info]
   (.connect (client/connection uri worker-info)))
  ([uri]
   (connect uri (worker-info))))

(defn- run-work-loop [worker]
  (let [conn (connect (get-in worker [::opts :uri]) (::info worker))
        queues (get-in worker [::opts :queues])]
    (loop []
      (when-not @(::stopped? worker)
        (when-let [{:keys [jid] :as job} (decode-transit-args (client/fetch conn queues))]
          (try
           (if-let [handler-fn (get @registered-jobs (keyword (:jobtype job)))]
             (do (apply handler-fn (:args job))
                 (client/ack conn jid))
             (throw (Exception. "No handler job type")))
           (catch Throwable e
             (prn "Throwable" e)
             (log/warn e)
             (client/fail conn jid e)))))
      (when-not @(::stopped? worker)
        (recur)))))

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
       (client/push (::conn worker) job)
       jid)
     (throw (Exception. "Job type has not been registered"))))
  ([worker job-type args]
   (perform-async worker job-type args {})))

(defn info [worker]
  (client/info (::conn worker)))

(defn worker [uri opts]
  (let [{:keys [concurrency heartbeat]
         :as   opts} (merge {:concurrency 10
                             :queues      ["default"]
                             :heartbeat   10000} opts)
        info (worker-info)
        conn (connect uri info)
        beat-pool (ScheduledThreadPoolExecutor. 1 daemon-thread-factory)
        beat-conn (connect uri info)
        work-pool (Executors/newFixedThreadPool concurrency)]
    (keep-alive conn beat-pool (:wid info) heartbeat)
    {::info info
     ::opts (assoc opts :uri uri)
     ::conn conn
     ::beat-pool beat-pool
     ::beat-conn beat-conn
     ::work-pool work-pool
     ::stopped? (atom false)}))

(defn stop [{:keys [::work-pool ::beat-pool ::beat-conn ::conn] :as worker}]
  (log/debug "Stopping worker")
  (try
   (reset! (::stopped? worker) true)
   (.shutdownNow beat-pool)
   (println 1)
   (.shutdown work-pool)
   (println 2)
   (.shutdownNow work-pool)
   (println 3)
   (when-not (.awaitTermination work-pool 10000 TimeUnit/MILLISECONDS)
     (throw (Exception. "Could not shut down work pool properly")))
   (println 4)
   (catch InterruptedException _
     (println 5)
     (.shutdownNow work-pool)
     (.interrupt (Thread/currentThread)))
   (finally
    (println 6)
    (.close @conn)
    (.close @beat-conn)
    (println 7)))
  worker)

(defn start [worker]
  (when @(::stopped? worker)
    (throw (Exception. "A stopped worker cannot be started again")))
  (dotimes [_ (get-in worker [::opts :concurrency])]
    (.submit (::work-pool worker) #(run-work-loop worker)))
  worker)
