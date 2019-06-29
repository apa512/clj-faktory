(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [clj-faktory.core :refer :all]))

(deftest core-test
  (testing "can add and process jobs"
    (let [processed-args (atom [])
          _ (defjob :save-args [a b]
              (swap! processed-args conj [a b]))
          queue-name (str "test-" (System/currentTimeMillis))
          worker (start (worker "tcp://localhost:7419" {:concurrency 1
                                                        :queues [queue-name]}))]
      (dotimes [n 10]
        (perform-async worker :save-args [:hello "world"] {:queue queue-name
                                                           :retry 0}))
      (stop worker)
      (is (= (count @processed-args) 10))
      (is (= (first @processed-args) [:hello "world"]))
      (reset! processed-args []))))
