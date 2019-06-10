(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [pool.core :as pool]
            [clj-faktory.core :refer :all]
            [clj-faktory.socket :as socket]))

(def processed-args
  (atom []))

(defn- save-args [a b]
  (swap! processed-args conj [a b]))

(deftest core-test
  (let [conn-pool (conn-pool "tcp://localhost:7419")
        queue-name (str "test-" (System/currentTimeMillis))
        worker-manager (worker-manager conn-pool {:concurrency 1
                                                  :heartbeat 10000000000
                                                  :queues [queue-name]})]
    (register-job :save-args save-args)
    (start worker-manager)
    (dotimes [n 10]
      (perform-async worker-manager :save-args [:hello "world"] {:queue queue-name
                                                                 :retry 0}))
    (stop worker-manager)
    (is (= (count @processed-args) 10))
    (is (= (first @processed-args) [:hello "world"]))
    (reset! processed-args [])))
