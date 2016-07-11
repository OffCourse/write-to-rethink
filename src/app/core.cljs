(ns app.core
  (:require [cljs.nodejs :as node]
            [app.db :as db]
            [app.event :as event]
            [cljs.core.match :refer-macros [match]]
            [cljs.core.async :refer [<! put! close! chan >!]]
            [clojure.string :as str])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(node/enable-util-print!)

(defn ^:export handler [event context cb]
  (println (.stringify js/JSON event))
  (go
    (let [{:keys [records event-source]} (event/convert event)
          {:keys [connection error]} (<! (db/connect))]
      (when connection
        (let [response (<! (db/insert connection "bookmarks" records))]
          (println (.stringify js/JSON (clj->js records)))
          (.close connection #(cb nil (clj->js response))))))))

(defn -main [] identity)
(set! *main-cli-fn* -main)
