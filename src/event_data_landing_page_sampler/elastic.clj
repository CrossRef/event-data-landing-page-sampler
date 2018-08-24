(ns event-data-landing-page-sampler.elastic
  (:require 
            [crossref.util.doi :as cr-doi]
            [crossref.util.string :as cr-str]
            [qbits.spandex.utils :as s-utils]
            [qbits.spandex :as s]
            [clj-time.coerce :as coerce]
            [clj-time.format :as clj-time-format]
            [clojure.tools.logging :as log]
            [robert.bruce :refer [try-try-again]]
            [config.core :refer [env]]
            [com.climate.claypoole :as cp]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [com.climate.claypoole :as cp])
  (:import [java.net URL MalformedURLException]
           [org.elasticsearch.client ResponseException]))

(def tries 10)

(def full-format (:date-time clj-time-format/formatters))

(defn normalize-doi-for-index
  "Normalize a DOI to a standard form for index.
   NB suffix is upper-cased before normalizing, so that the scheme and resolver aren't affected."
  [doi]
  (some-> doi cr-doi/non-url-doi string/upper-case cr-doi/normalise-doi))

(def pid-mappings
  {:pid/last-snapshot {:type "date" :format "epoch_millis" :null_value 0}
   :pid/timestamp {:type "date" :format "epoch_millis"}
   :pid/doi {:type "keyword"}
   :pid/doi-prefix {:type "keyword"}
   :pid/global-member-id {:type "keyword"}
   :pid/member-id {:type "keyword"}
   :pid/internal-member-id {:type "keyword"}
   :pid/ra {:type "keyword"}
   :pid/resource-url {:type "keyword"}})


(def snapshot-mappings
  (merge pid-mappings
    {:snapshot/last-analysis {:type "date" :format "epoch_millis"  :null_value 0}
     :snapshot/timestamp {:type "date" :format "epoch_millis"}
     :snapshot/content-hash {:enabled false}
     :snapshot/urls {:enabled false}
     :snapshot/final-url-domain {:type "keyword"}
     :snapshot/final-url {:type "keyword"}
     :snapshot/driver {:type "keyword"}}))

(def analysis-mappings
  (merge snapshot-mappings
    {:analysis/meta-tags {:enabled false}
     :analysis/meta-tag-correctness {:type "keyword"}
     :analysis/doi-in-text {:type "keyword"}
     :analysis/doi-in-hyperlinks {:type "keyword"}
     :analysis/doi-in-meta-tags {:type "keyword"}}))

(def member-report-mappings
  "Member report is just used to store the resut, indexed by the member id.
   TODO in future this could be indexed to allow querying by scores."
  {:report {:type "object" :enabled false}})

(def domain-report-mappings
  {:report {:type "object" :enabled false}})

(def prefix-report-mappings
  {:report {:type "object" :enabled false}})

(def index-configs
  {:pid {:name "pid" :mappings pid-mappings}
   :snapshot {:name "snapshot" :mappings snapshot-mappings}
   :analysis-mappings {:name "analysis" :mappings analysis-mappings}
   :member-report-mappings {:name "member-report" :mappings member-report-mappings}
   :prefix-report {:name "prefix-report" :mappings prefix-report-mappings}
   :domain-report {:name "domain-report" :mappings domain-report-mappings}})

(def connection (delay
                 (s/client {:hosts [(:landing-page-sampler-elastic-uri env)]
                            :max-retry-timeout 60000
                            :request {:connect-timeout 60000
                                      :socket-timeout 60000}})))

(defn ensure-index
  [index-config]
  (try
    (s/request @connection {:url (:name index-config) :method :head})
    (catch Exception ex
      (log/info "Need to create index" (:name index-config) "with mappings" (:mappings index-config))
      (try
        (s/request @connection {:url (:name index-config)
                                :method :put
                                :body {:settings {"number_of_replicas" 0}
                                       :mappings {"data" {:properties (:mappings index-config)}}}})
        (catch Exception ex2
          (log/error "Failed to create index!" ex2))))))

(defn ensure-indexes
  []
  (log/info "Ensuring indexes...")
  (doseq [index-config (vals index-configs)]
    (log/info "Ensuring index" (:name index-config))
    (ensure-index index-config))
  (log/info "Finished ensuring indexes."))

(defn close! []
  (s/close! @connection))



(def acceptable-index-status-codes
  "We expect a variety of status codes back from an ElasticSearch 'index' action.
   Any others should be treated as errors."
  #{; Conflict. Ok, as we have an update strategy.
    409 
    ; Created.
    201 
    ; Ok.
    200 })

(def batch-size 100)

(defn insert-docs
  "Insert a batch of objects."
  [index-name docs]
   (when-not (empty? docs)
     (let [chunks (partition-all batch-size docs)]
      (doseq [chunk chunks]

            (let [bulk-docs (mapcat
                              (fn [document]
                                 [{:index {
                                    :_index index-name
                                    :_type "data"}}
                                  document]) chunk)

                    result   (try-try-again
                               {:sleep 5000 :tries tries}
                               #(s/request @connection {:url "_bulk"
                                           :method :post
                                           :body (s/chunks->body bulk-docs)}))

                  items (-> result :body :items)

                  problem-items (remove #(-> % :index :status acceptable-index-status-codes) items)]

              (when (not-empty problem-items)
                (log/error "Unexpected response items:" problem-items)
                (throw (Exception. "Unexpected response items"))))))))

(defn insert-docs-ids
  "Insert a batch of [_id content] pairs."
  [index-name id-docs]
  (when-not (empty? id-docs)
    (let [chunks (partition-all batch-size id-docs)]
      (doseq [chunk chunks]
        (log/info "insert chunk of..." (count chunk) "first" (take 2 chunk))
        (let [bulk-docs (mapcat
                          (fn [[id document]]
                             [{:create {
                                :_id id
                                :_index index-name
                                :_type "data"}}
                              document]) chunk)
                result (try-try-again
                           {:sleep 60000 :tries tries}
                           (fn []
                            ; (log/info "Insert" (first bulk-docs))
                            (s/request @connection {:url "_bulk"
                                       :method :post
                                       :body (s/chunks->body bulk-docs)})))

              items (-> result :body :items)

              problem-items (remove #(-> % :create :status acceptable-index-status-codes) items)]

          (when (not-empty problem-items)
            (log/error "Unexpected response items:" problem-items)
            (throw (Exception. "Unexpected response items"))))
        (log/info "inserted chunk of" (count chunk) "first" (take 2 chunk))
            ))))


(defn unsnapshotted-pids
  "Retrieve some PIDs that have not been snapshotted yet."
  []
  ; Flush any pending changes.

  (s/request
      @connection
      {:url ["pid" "data" "_flush"]
      :method :post
      :body {}})
  (s/request
      @connection
      {:url ["pid" "data" "_refresh"]
      :method :post
      :body {}})


  (->>
    (s/request
      @connection
      {:url ["pid" "data" "_search"]
       :method :post
       :body {:size 100 
              :query {:match {:pid/last-snapshot 0}}}})
     :body
     :hits
     :hits))

(defn insert-pid-snapshot
  [id snapshot]
  (let [now (System/currentTimeMillis)
        doi (:pid/doi snapshot)]
    
    (try-try-again {:sleep 5000 :tries tries}
                   #(s/request @connection {:url ["snapshot" "data" id]
                     :method :put
                     :body snapshot}))

    (try-try-again {:sleep 5000 :tries tries}
     #(s/request @connection
        {:url ["pid" "data" "_update_by_query"]
         :query-string {"conflicts" "proceed"}
         :method :post
         :body {:query {:match {:pid/doi doi}}
                :script {:source (str "ctx._source[\"pid/last-snapshot\"] = params.timestamp") :params {:timestamp now}}}}))))

(defn insert-analysis
  [id analysis update-snapshot]
  (let [now (System/currentTimeMillis)
        doi (:pid/doi analysis)]
    
    (try-try-again {:sleep 5000 :tries tries}
                   #(s/request
                   @connection {:url ["analysis" "data" id]
                                :method :put
                                :body analysis}))

    ; This is only useful when we update after the fact. If we analyze on creation of the snapshot then it will be created with a value set.
    (when update-snapshot
      (try-try-again {:sleep 5000 :tries tries}
                     #(s/request @connection
                                  {:url ["snapshot" "data" "_update_by_query"]
                                   :query-string {"conflicts" "proceed"}
                                   :method :post
                                   :body {:query {:match {:pid/doi doi}}
                                          :script {:source (str "ctx._source[\"snapshot/last-analysis\"] = params.timestamp") :params {:timestamp now}}}})))))



(def facet-fetch-size 100)

(defn facet-count
  ([index-name field query]
    ; (log/info "Start facet count for" index-name "on" field "with query" query)
    (facet-count index-name field query nil))
  ([index-name field query after]
    ; (log/info "Recurse facet count for" index-name "on" field "with query" query "after key" after)
    (let [composite {:size facet-fetch-size
                                    :sources [
                                        { :facet-field { :terms { :field field }}}]}
          
          ; If an 'after' was supplied for pagination, include that.
          composite (if after (assoc composite :after after)
                              composite)

          body {:size 0
                :aggs {:buckets {:composite composite}}}

          ; Query be nil, but that's not allowed.
          body (if query (assoc body :query query) body)

          result (try-try-again {:sleep 5000 :tries tries} #(s/request @connection {:url [index-name "_search"] :body body}))

          facets (->> result :body :aggregations :buckets :buckets (map #(vector (-> % :key :facet-field) (:doc_count %))))
          
          last-key (->> result :body :aggregations :buckets :buckets last :key)]
    ; (log/info "Last key" last-key)
    (if-not last-key
      facets
      (lazy-cat facets (facet-count index-name field query last-key))))))

(defn query-count
  [index-name query]
    (let [body {}

          ; Query be nil, but that's not allowed.
          body (if query (assoc body :query query) body)

          result (try-try-again {:sleep 5000 :tries tries} #(s/request @connection {:url [index-name "_search"] :body body}))]
      (-> result :body :hits :total )))


(defn every-item
  "Run callback for every item in the index"
  [index-name parallelism callback]
  ; Take a batch size as a multiple of paralleism so each parallel partition has at least this many to work on.
  (let [batch-size (* parallelism 100)
        ch (try-try-again {:sleep 5000 :tries tries}
                            #(s/scroll-chan @connection {:url [index-name "_search"] :ttl "60m" :body {:size batch-size :query {:match_all {}}}}))]
    (loop []
      (log/info "Every item page for" index-name)
      (if-let [page (async/<!! ch)]
        (let [hits (-> page :body :hits :hits)]
          (cp/pdoseq parallelism [hit hits]
            (callback hit))
          (recur))
        (log/info "Finished!")))))

(defn insert-report
  [id report]
  (try-try-again {:sleep 5000 :tries tries}
                 #(s/request
                 @connection {:url ["member-report" "data" id]
                              :method :put
                              :body report})))
