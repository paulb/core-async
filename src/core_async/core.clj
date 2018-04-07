(ns core-async.core
  (:require [clojure.core.async :as a :refer [>! >!! <! <!! alts!
                                              chan go go-loop timeout]]))

;; Basic channel examples.

(defn basic-comms
  "Sets up a simple listening channel.
  Prints messages as they are received.
  Prints an exit message when the input channel is closed."
  []
  (let [in (chan 100)]
    (go-loop
      []
      (if-let [message (<! in)]
        (do
          (println :received-message message)
          (recur))
        (println :channel-closed)))
    in))

(defn send-messages
  "Sends a single list of messages to the supplied channel."
  [ch messages]
  (doseq [message messages]
    (>!! ch message)))

(defn send-random-delayed-messages
  "Sends an infinite stream of messages to the supplied channel
  at random intervals. Stops sending when the channel is closed."
  [ch]
  (let [messages (repeatedly #(str "Message ID " (rand-int 1000)))]
    (go-loop
      [messages messages]
      (let [message (first messages)]
        (when-let [sent (>! ch message)]
          (println :sent-message message)
          (Thread/sleep (rand-int 5000))
          (recur (rest messages)))))))

;; Using core.async to implement a timed auto-save function.

(defn- auto-save
  "Runs a timeout for saving data.
  Caches data as it is received.
  When the timeout interval is reached, cached data is saved
  and a new timer is started."
  [in save-interval]
  (go-loop [latest-data []
            timer (timeout save-interval)]
    (let [[data ch] (alts! [in timer])]
      (condp = ch
        timer
        (do
          (when (seq latest-data)
            (println :timeout-triggered)
            (println :saving-data! latest-data))
          (recur [] (timeout save-interval)))

        in
        (when data
          (println :received-new-data)
          (println :caching-data! data)
          (recur (conj latest-data data) timer))))))

(defn init-save
  "Initialises auto-save and returns a function to stop."
  [save-interval]
  (let [from-ch (chan 100)]
    (auto-save from-ch save-interval)
    from-ch))

;; Pipeline example for batched parallel processing.

(defn pipeline
  "Initialises a pipeline which will take messages from the from-ch,
  pass them to the message-handler function and send the output to
  the to-ch.
  Instantiates a listener which will take messages from the to-ch
  and print the result of processing the message while tracking
  overall progress. When the from-ch is closed and there are no more
  messages to take from the from-ch, the listener will put the final
  status on the done-ch.
  Returns the done-ch and a function to close the from-ch."
  [from-ch concurrency message-handler]
  (let [to-ch (chan 100)
        done-ch (chan)]
    (a/pipeline concurrency to-ch (map message-handler) from-ch)

    (go-loop
      [processed 0
       successful 0]
      (let [{:keys [status message]} (<! to-ch)]
        (if status
          (let [successful (if (= status :success)
                             (inc successful)
                             successful)]
            (println :processed message :status status)
            (recur (inc processed) successful))
          (do
            (println :pipeline-closed)
            (>! done-ch {:processed processed
                         :successful successful
                         :failed (- processed successful)})))))

    {:done-ch done-ch
     :stop (fn [] (a/close! from-ch))}))

(defn message-handler
  "Handles individual messages. Randomly passes or fails messages
  and sleeps between messages for demonstration purposes."
  [message]
  (println :handler-received-message message)
  (let [chance (rand-int 2)
        result {:status (if (= chance 1) :success :failed)
                :message message}]
    (Thread/sleep (+ (rand-int 5000) 5000))
    result))

(defn pipeline-feeder
  "Instantiates a pipeline and a listener which will receive final
  status of processing all messages and output to the user.
  Returns the stop function for terminating the pipeline."
  []
  (let [from-ch (chan 100)
        concurrency 5
        {:keys [done-ch stop]} (pipeline from-ch concurrency message-handler)
        messages (reduce #(conj %1 (str "Message ID " %2)) [] (range 1 26))]
    (go (when-let [{:keys [processed successful failed]} (<! done-ch)]
          (println :received-final-status)
          (println :total-messages processed)
          (println :successful-messages successful)
          (println :failed-messages failed)))

    (go (doseq [message messages]
          (>! from-ch message)))

    stop))

;; Don't do this!
;; While everything will seem normal as long as you keep the channel open,
;; the loop responding to data sent to the channel as though all is well,
;; when the channel is closed the process will enter an infinite loop.
;; This is because there is no conditional clause on the recur. The loop is
;; paused between inputs only while the channel is parked. Once it is closed
;; it will return nil on every call instead of waiting for data.

(defn infinite-loop
  []
  (let [ch (chan 100)]
    (go-loop
      [counter 0]
      (let [data (<! ch)]
        (println :processed counter)
        (recur (inc counter))))
    ch))
