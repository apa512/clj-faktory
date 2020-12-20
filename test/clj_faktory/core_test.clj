(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [clj-faktory.client :as client]
            [clj-faktory.core :refer :all]
            [clj-faktory.test-utils :refer [wait-until]]))

(deftest core-test
  (testing "can add and process jobs"
    (let [processed-args (atom [])
          _ (defjob :save-args [a b] (swap! processed-args conj [a b]))
          queue-name (str "test-" (System/currentTimeMillis))
          worker (start (worker "tcp://localhost:7419" {:concurrency 1
                                                        :queues [queue-name]}))]
      (dotimes [_ 10]
        (perform-async worker :save-args [:hello "world"] {:queue queue-name
                                                           :retry 0}))
      (is (wait-until (= (count @processed-args) 10)))
      (stop worker)
      (is (= (first @processed-args) [:hello "world"]))
      (reset! processed-args []))))
