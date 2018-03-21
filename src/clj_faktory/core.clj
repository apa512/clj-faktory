(ns clj-faktory.core
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as cheshire]
            [crypto.random :as random]
            [pool.core :as pool]
            [clj-faktory.protocol.transit :as transit]
            [clj-faktory.socket :as socket])
  (:import [java.util.concurrent Executors ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

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
  (socket/with-conn [conn conn-pool]
    (socket/send-command conn command)))

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

(defn- run-work-loop [{:keys [conn-pool queues]} n]
  (loop []
    (log/debug "Worker" n "checking in")
    (let [{:keys [jid] :as job} (decode-transit-args (fetch conn-pool queues))
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
        :success (do (ack conn-pool jid)
                     (recur))
        :failure (do (fail conn-pool jid e)
                     (recur))
        :stopped (fail conn-pool jid e)
        (recur)))))

(defn- keep-alive [{:keys [conn-pool wid heartbeat] :as worker-manager}]
  (let [thread-factory (reify ThreadFactory
                         (newThread [_ runnable]
                           (doto (Thread. runnable)
                             (.setDaemon true))))]
    (.scheduleWithFixedDelay (ScheduledThreadPoolExecutor. 1 thread-factory)
                             (fn []
                               (log/debug "❤❤❤")
                               (beat conn-pool wid))
                             heartbeat
                             heartbeat
                             TimeUnit/MILLISECONDS)))

(defn stop [{:keys [conn-pool worker-pool :as worker-manager]}]
  (try
   (when-not (.awaitTermination worker-pool 2000 TimeUnit/MILLISECONDS)
     (.shutdownNow worker-pool))
   (catch InterruptedException e
     (log/debug e)
     (.shutdownNow worker-pool))
   (finally
    (pool/close conn-pool)))
  worker-manager)

(defn start [{:keys [worker-pool concurrency] :as worker-manager}]
  (keep-alive worker-manager)
  (dotimes [n concurrency]
    (.submit worker-pool #(run-work-loop worker-manager n)))
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
   (let [worker-pool (Executors/newFixedThreadPool concurrency)]
     (merge conn-pool
            {:worker-pool worker-pool
             :concurrency concurrency
             :heartbeat heartbeat
             :queues queues})))
  ([conn-pool]
   (worker-manager conn-pool {})))
