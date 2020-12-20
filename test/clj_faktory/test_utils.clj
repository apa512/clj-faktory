(ns clj-faktory.test-utils)

(defn wait-until*
  ([name f] (wait-until* name f 5000))
  ([name f wait-death]
   (let [default-wait-delay-ms 10
         remaining-ms (atom wait-death)]
     (loop []
       (if-let [result (f)]
         result
         (do
           (Thread/sleep default-wait-delay-ms)
           (if (<= @remaining-ms 0)
             (throw (Exception. (str "Timed out waiting for: " name)))
             (do (swap! remaining-ms - default-wait-delay-ms)
                 (recur)))))))))

(defmacro wait-until
  [expr]
  `(wait-until* ~(pr-str expr) (fn [] ~expr)))
