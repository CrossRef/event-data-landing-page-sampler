(ns event-data-landing-page-sampler.sample-crossref
  "Collect a representative sample of DOIs per member from Crossref.
   As Crossref has an internal sample function, use this to collect random samples."
  (:require [org.httpkit.client :as client]
            [event-data-landing-page-sampler.http :as http]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [robert.bruce :refer [try-try-again]])
  (:gen-class))

(def api-base "https://api.crossref.org/")

(def member-page-size 1000)

(defn all-members
  "Return a seq of [member-id, num-dois]."
  ([] (doall (sort-by first (all-members 0))))
  ([offset]
    (try-try-again
      {:sleep 1000 :tries 5}
      (fn []
        (log/info "Fetch all members, offset" offset)
        (let [response @(client/get
                          (str api-base "members")
                          {:query-params {
                           :rows member-page-size
                           :offset offset
                           :mailto "eventdata@crossref.org"}
                           :as :text
                           :header http/headers
                           :client http/sni-client})
              body (json/read-str (:body response) :key-fn keyword)
              members (-> body :message :items)
              ; A potential REST API bug sometimes returns nil for count.
              ; Default to 1.
              results (map #(vector (:id %) (or (-> % :counts :total-dois) 1)) members)]
          (if (empty? results)
            []
            (lazy-cat results (all-members (+ offset member-page-size)))))))))


; We're limited by the REST API to 100.
(def max-sample 100)

; Always take at least this many samples.
(def min-sample 10)

(defn num-samples-per-member
  "Transform seq of [member-id num-dois] to [member-id num-samples]"
  [members-and-counts]
  (let [max-value (apply max (map second members-and-counts))
        scale-factor (/ max-sample max-value)]
    (map #(vector (first %)
                  (max min-sample 
                      (int (* scale-factor (second %)))))
    members-and-counts)))

(defn doi-sample-for-member
  "Return a seq of a sample of [member-id doi] for the member."
  [[member-id num-samples]]
  (log/info "Sample " num-samples " DOIs for member" member-id)
  (try-try-again
    {:sleep 10000 :tries 5}
    (fn []
      (let [response @(client/get
                        (str api-base "works")
                        {:query-params {:sample num-samples
                                        :select "DOI"
                                        :filter (str "member:" member-id)
                                        :mailto "eventdata@crossref.org"}
                         :as :text})
        body (json/read-str (:body response) :key-fn keyword)
        works (-> body :message :items)
        dois (map :DOI works)]
      (map #(vector member-id %) dois)))))


(defn sample-all
  "Return a randomly shuffled sample of [member-id, doi] for all members."
  []
  (let [; Order doesn't matter, so sort by member ID so we know how far we are through the job.
        members-and-counts (sort-by first (all-members))
        members-and-samples (num-samples-per-member members-and-counts)
        doi-sets (pmap doi-sample-for-member members-and-samples)
        dois (mapcat identity doi-sets)
        shuffled (shuffle dois)]
    shuffled))
