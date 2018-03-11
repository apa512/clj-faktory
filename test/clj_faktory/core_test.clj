(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [pool.core :as pool]
            [clj-faktory.core :refer :all]
            [clj-faktory.socket :as socket]))

(def processed-args (atom []))

(defn save-args [a b]
  (swap! processed-args conj [a b]))

(deftest core-test
  (let [conn-pool (conn-pool "tcp://localhost:7419")
        worker-manager (worker-manager conn-pool {:concurrency 4
                                                  :queues ["test" "default"]})]
    (register-job :save-args save-args)
    (start worker-manager)
    (dotimes [n 10]
      (perform-async worker-manager :save-args [:hello "world"] {:queue "test"
                                                                 :retry 0}))
    (Thread/sleep 300)
    (is (= (count @processed-args) 10))
    (is (= (first @processed-args) [:hello "world"]))
    (stop worker-manager)
    (reset! processed-args [])))
