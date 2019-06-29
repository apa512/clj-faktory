(ns clj-faktory.core
  (:require [clj-faktory.worker :as worker]))

(def perform-async worker/perform-async)
(def worker        worker/worker)
(def start         worker/start)
(def stop          worker/stop)

(defmacro defjob [job-type params & body]
  `(worker/register-job ~job-type (fn ~params ~@body)))
