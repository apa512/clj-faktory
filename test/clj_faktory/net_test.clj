(ns clj-faktory.net-test
  (:require [clojure.test :refer :all]
            [clj-faktory.net :refer :all]))

(deftest net-test
  (testing command-str
    (are [x y] (= y (command-str x))
         [:hi] "HI"
         [:fetch "high" "default" "low"] "FETCH high default low")))
