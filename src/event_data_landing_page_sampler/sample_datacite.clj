(ns event-data-landing-page-sampler.sample-datacite
  "Collect a representative sample of DOIs per prefix.
   As the DataCite API doesn't have random samples, take a randomly selected page."
  (:require [org.httpkit.client :as client]
            [event-data-landing-page-sampler.http :as http]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [robert.bruce :refer [try-try-again]])
  (:gen-class))

(defn all-prefixes
  "Return a seq of [prefix, num-dois]."
  []
  (try-try-again
    {:sleep 1000 :tries 5}
    (fn []
      (let [response (client/get "https://search.datacite.org/list/generic?&facet.field=prefix"
                             {:as :text :headers http/headers :client http/sni-client})

            lines (-> response
                       deref
                       :body
                       (.split "\n"))

            result (->> lines
                       (map (partial re-find #"^(10\.\d+); (\d+);$"))
                       (map (fn [[_ prefix cnt]]
                              [prefix (Integer/parseInt cnt)])))]
        result))))


; We're limited by the REST API to 100.
(def max-sample 100)

; Always take at least this many samples.
(def min-sample 10)

(defn num-samples-per-prefix
  "Transform seq of [prefix num-dois] to [prefix random-offset num-samples]"
  [prefixes-and-counts]
  (let [max-value (apply max (map second prefixes-and-counts))
        scale-factor (/ max-sample max-value)]
    (map (fn [[prefix num-dois]]
           (let [num-samples (max min-sample 
                                  (int (* scale-factor num-dois)))
                 ; Might be fewer DOIs than min-sample.
                 random-offset (max 0 (int (Math/floor (* (Math/random) (- num-dois num-samples)))))]
            (println prefix num-dois random-offset num-samples)
             [prefix random-offset num-samples]))
          prefixes-and-counts)))

(defn doi-sample-for-prefix
  "Return a seq of a sample of [prefix, doi] for the prefix."
  [[prefix random-offset num-samples]]
  (try-try-again
    {:sleep 5000 :tries 2}
    (fn []
      (log/info "Sample " num-samples " DOIs for prefix" prefix)
      (let [response @(client/get "https://api.datacite.org/works"
                               {:query-params {:rows num-samples
                                               :offset random-offset
                                               :query (str "prefix:" prefix)}
                                :as :text})
            body (json/read-str (:body response) :key-fn keyword)
            works (-> body :data)
            dois (map :id works)]
        (map #(vector prefix %) dois)))))

(defn sample-all
  "Return a randomly shuffled sample of [member-id, doi] for all prefixes.
   Prefixes are treated as members."
  []
  (let [; Order doesn't matter, so sort by prefix ID so we know how far we are through the job.
        prefixes-and-counts (sort-by first (all-prefixes))
        prefixes-and-samples (num-samples-per-prefix prefixes-and-counts)
        doi-sets (pmap doi-sample-for-prefix prefixes-and-samples)
        dois (apply concat doi-sets)
        shuffled (shuffle dois)]
    shuffled))
