(ns clj-faktory.core
  (:require [clj-faktory.publisher :as publisher]
            [clj-faktory.worker :as worker]))

(def perform-async worker/perform-async)
(def publish       publisher/publish)
(def publisher     publisher/publisher)
(def worker        worker/worker)
(def start         worker/start)
(def stop          worker/stop)
(def info          worker/info)

(defmacro defjob [job-type params & body]
  `(worker/register-job ~job-type (fn ~params ~@body)))
