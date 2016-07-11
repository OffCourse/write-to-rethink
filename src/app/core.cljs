(ns app.core
  (:require [cljs.nodejs :as node]
            [app.db :as db]
            [app.event :as event]
            [app.specs :as specs]
            [cljs.core.match :refer-macros [match]]
            [cljs.core.async :refer [<! put! close! chan >!]]
            [cljs.spec :as spec])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(node/enable-util-print!)

(defn handle-error [reason payload cb]
  (let [error (clj->js {:type :error
                        :error reason
                        :payload payload})]
    (println (.stringify js/JSON error))
    (cb error nil)))

(defn process-event [connection event cb]
  (if (spec/valid? ::specs/event event)
    (go
      (let [{:keys [payload type]}     (spec/conform ::specs/event event)
            [payload-type items]       payload
            response (<! (db/insert connection (name payload-type) items))]
        (.close connection #(cb nil (clj->js response)))))
    (handle-error :specs-not-matched (spec/explain-data ::specs/event event) cb)))

(defn ^:export handler [raw-event context cb]
  (println (.stringify js/JSON raw-event))
  (go
    (let [event                              (event/convert raw-event)
          {:keys [connection error] :as res} (<! (db/connect))]
      (println (.stringify js/JSON (clj->js event)))
      (match [res]
             [{:error _}] (handle-error error event cb)
             [{:connection _}] (process-event connection event cb)))))

(defn -main [] identity)
(set! *main-cli-fn* -main)
