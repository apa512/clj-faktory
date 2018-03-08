(ns clj-faktory.core
  (:require [crypto.random :as random]
            [pool.core :as pool]
            [clj-faktory.socket :as socket]))

(defmulti perform (comp keyword :jobtype))

(defmethod perform :default [job]
  (throw (Exception. (str "No handler for job type " (keyword (:jobtype job))))))

(defn- send-command [conn-pool command]
  (try
   (socket/with-conn [conn conn-pool]
     (socket/send-command conn command))
   (catch java.lang.IllegalStateException _
     :closed)))

(defn- apply-interceptors [job interceptors k]
  (->> interceptors
       (map k)
       (remove nil?)
       (reduce #(%2 %1) job)))

(defn- fail [conn-pool job e]
  (send-command conn-pool [:fail {:jid (:jid job)
                                  :message (.getMessage e)
                                  :errtype (str (class e))
                                  :backtrace (map #(.toString %) (.getStackTrace e))}]))

(defn- run-work-loop [{:keys [conn-pool queues interceptors]}]
  (future
   (loop []
     (let [job (send-command conn-pool (cons :fetch queues))]
       (when-not (= job :closed)
         (when job
           (let [job (apply-interceptors job interceptors :leave)]
             (try
              (perform job)
              (send-command conn-pool [:ack {:jid (:jid job)}])
              (catch Exception e
                (fail conn-pool job e)))))
         (recur))))))

(defn stop [{:keys [conn-pool]}]
  (pool/close conn-pool))

(defn start [worker-manager]
  (dotimes [_ (:concurrency worker-manager)]
    (run-work-loop worker-manager))
  worker-manager)

(def conn-pool socket/conn-pool)

(defn perform-async
  ([{:keys [conn-pool interceptors]} job-type args opts]
   (let [job (-> {:jid (random/hex 12)
                  :jobtype job-type
                  :args args
                  :retry 25
                  :backtrace 10}
                 (merge opts)
                 (apply-interceptors interceptors :enter))]
     (send-command conn-pool [:push job])))
  ([worker-manager job-type args]
   (perform-async worker-manager job-type args {})))

(defn worker-manager
  ([conn-pool {:keys [concurrency
                      heartbeat
                      queues
                      interceptors] :or {concurrency 10
                                         heartbeat 15000
                                         queues ["default"]}}]
   (merge conn-pool
          {:concurrency concurrency
           :heartbeat heartbeat
           :queues queues
           :interceptors interceptors}))
  ([conn-pool]
   (worker-manager conn-pool {})))
