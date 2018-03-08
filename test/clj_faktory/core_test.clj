(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [pool.core :as pool]
            [clj-faktory.core :refer :all]
            [clj-faktory.interceptors :refer [transit-args]]
            [clj-faktory.socket :as socket]))

(def finished-jobs (atom []))

(defmethod perform :save-job [job]
  (swap! finished-jobs conj job)
  (throw (Exception. "Hora")))

(def test-interceptor
  {:leave (fn [job]
            (assoc job :test 1234))})

(deftest core-test
  (let [conn-pool (conn-pool "tcp://localhost:7419")
        worker-manager (worker-manager conn-pool {:concurrency 4
                                                  :interceptors [test-interceptor
                                                                 transit-args]
                                                  :queues ["test" "default"]})]
    (start worker-manager)
    (dotimes [n 10]
      (perform-async worker-manager :save-job [:hello "world"] {:queue "test"
                                                                :retry 0}))
    (Thread/sleep 200)
    (is (= (count @finished-jobs) 10))
    (is (= (get-in @finished-jobs [0 :test]) 1234))
    (is (= (get-in @finished-jobs [0 :args]) [:hello "world"]))
    (reset! finished-jobs [])
    (stop worker-manager)))
