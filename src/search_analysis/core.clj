(ns search-analysis.core
  (:require [clojure.data.json :as json]
            [clojure.set :as set])
  (:use [clojure.java.io :only (reader writer delete-file)]
        [clojure.string :only [split]]))

(defn debug [x] (println x) x)

(defn read-index [line]
  (-> line (split #"=") first .trim))

(defn read-index-map []
  (with-open [rdr (reader "index.map")]
    (->> rdr line-seq (map read-index) doall)))



(defn verify-event [[[id1 json1]  [id2 json2]]]
  (if (and (->> [id1 id2] (some nil?) not)
           (not (= id1 id2)))
    (-> "Inconsistent input, terminating " (str id1 ", ", id2) RuntimeException. throw)
    [json1 json2]))

(defn parse-row [row]
  (if (= "null" row) [nil nil]
    (let [id (-> row (split #" ") first)
          offset (-> id count inc)]
      [(read-string id) (-> row (.substring offset) json/read-json)])))

(defn changed-keys [indexes m1 m2]
  (let [all-keys (distinct (concat (keys m1) (keys m2)))
        modification (->> all-keys
                          (map #(if (= (% m1) (% m2)) nil %))
                          (filter (comp not nil?))
                          (map #(.substring (str %) 1))
                          (into #{})
                          )]
    (set/intersection modification indexes)))

(def DOCS-CHANGES-INFO "/Volumes/flash/docs-changes-info")
(def DOCS-INFO "/Volumes/flash/docs-info")

(defn get-modification-info [writer indexes json1 json2]
  (let [changed (vec (changed-keys indexes (:attributes json1) (:attributes json2)))]
    (if (-> changed empty? not)
      (.write writer
              (apply str
                     (into
                      [(json1 :id) \tab (-> json1 :attributes :type first) \tab]
                      (conj
                       (vec (interpose \space changed))
                       \newline)))))
    (->> changed
         (map #(vector % 1))
         (into {}))))

(defn diff-event-processor [writer indexes [json1 json2]]
  (cond
   (and (nil? json1) (nil? json2)) {:zen 1}
   (nil? json1) {:event-added 1}
   (nil? json2) {:event-deleted 1}
   :else (let [info (get-modification-info writer indexes json1 json2)]
           (conj info
                 (if (empty? info)
                   {:event-not-modified 1}
                   {:event-modified 1})))))

(defn event-info-merge [coll1 coll2]
  (merge coll1 coll2))

(defn show-fields [coll]
  (->> coll
       (map #(str (key %) \tab (val %)))
       (interpose \newline)
       (apply str)))

(defn show [coll]
  (println coll)
  (str "deleted: " (:event-deleted coll) \newline
       "added: " (:event-added coll) \newline
       "null/null: " (:zen coll) \newline
       "modified: " (:event-modified coll) \newline
       "not modified: " (:event-not-modified coll) \newline
       \newline
       (show-fields
        (dissoc coll :zen :event-deleted :event-added :event-modified :event-not-modified))))

(defn print-state-info [coll]
  (let [modified (get coll :event-modified 0)
        deleted (get coll :event-deleted 0)
        added (get coll :event-added 0)
        not-modified (get coll :event-not-modified 0)
        sum (+ modified deleted added)]
    (if (-> sum (/ 10000) zero?)
      (println "Count: " sum))
    coll))

(defn analyze [file]
  (spit DOCS-CHANGES-INFO "")
  (spit DOCS-INFO "")
  (let [indexes (into #{} (read-index-map))]
    (with-open [rdr (reader file)
                wrt (-> DOCS-CHANGES-INFO
                        java.io.FileWriter.
                        (java.io.BufferedWriter. 10000))]
      (->> rdr
           line-seq
           (map parse-row)
           (partition 2)
           (map verify-event)
           print-state-info
           (map (partial diff-event-processor wrt indexes))
           (apply merge-with +)
           show
           (spit DOCS-INFO)))))


(require '[clojure.java.io :as jio])

;; (defn spit2 [f content]
;;   (with-open [#^java.io.Writer w (apply jio/writer f options)]
;;     (.write w (str content))))


