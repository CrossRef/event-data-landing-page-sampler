(ns event-data-landing-page-sampler.core
  (:require [event-data-landing-page-sampler.sample-crossref :as sample-crossref]
            [event-data-landing-page-sampler.sample-datacite :as sample-datacite]
            [event-data-landing-page-sampler.elastic :as elastic]
            [event-data-landing-page-sampler.html :as html]
            [event-data-landing-page-sampler.snapshot-http-kit :as snapshot-http-kit]
            [crossref.util.doi :as cr-doi]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [event-data-common.storage.s3 :as s3]
            [event-data-common.storage.store :as store]
            [config.core :refer [env]]
            [com.climate.claypoole :as cp]
            [clojure.java.io :refer [writer]])
  (:gen-class))

(def snapshot-store
  (delay
    (s3/build (:landing-page-sampler-s3-key env)
              (:landing-page-sampler-s3-secret env)
              (:landing-page-sampler-s3-region-name env)
              (:landing-page-sampler-s3-bucket-name env))))

(def parallelism 50)

(defn path-for-snapshot
  [snapshot]
  (str "event-data-landing-page-sampler/snapshot/" (:snapshot/content-hash snapshot)))


(defn member-doi-to-pid-entry
  "Build a 'pid' Elasticsearch document."
  [ra [member-id doi]]
  {:pid/last-snapshot nil
   :pid/timestamp (System/currentTimeMillis)
   :pid/doi-prefix (cr-doi/get-prefix doi)
   :pid/doi (-> doi cr-doi/non-url-doi clojure.string/lower-case)
   :pid/global-member-id (str ra "/" member-id)
   :pid/member-id member-id
   :pid/ra ra})

(defn sample-members
  []
  (elastic/ensure-indexes)
  (let [crossref-dois (sample-crossref/sample-all)
        datacite-dois (sample-datacite/sample-all)]
      (elastic/insert-docs :pid (map (partial member-doi-to-pid-entry "crossref") crossref-dois))
      (elastic/insert-docs :pid (map (partial member-doi-to-pid-entry "datacite") datacite-dois))))

(defn analyze
  "Perform analysis on a snapshot, given its snapshot object and its HTML content."
  [snapshot content]
  (let [meta-tags (html/identifier-doi-tags-present content)
        unambiguous (html/unambiguous-doi meta-tags)
        doi (:pid/doi snapshot)
        unambiguous-doi (when (and unambiguous
                                   (not= unambiguous "ambiguous"))
                          unambiguous)
        
        meta-tag-correctness (cond
                               (nil? unambiguous) "missing"
                               (= "ambiguous" unambiguous) "conflict"
                               (= (clojure.string/lower-case unambiguous) (clojure.string/lower-case doi)) "correct"
                               :default "incorrect")

        doi-in-text (html/doi-in-html-text doi content)
        doi-in-hyperlinks (html/doi-in-hyperlinks doi content)
        doi-in-meta-tags (html/doi-in-meta-tags doi content)]
      (merge snapshot
        {:analysis/meta-tags meta-tags
         
         ; This always has a value.
         :analysis/meta-tag-correctness meta-tag-correctness
         
         ; These three can be null.
         :analysis/doi-in-text doi-in-text
         :analysis/doi-in-hyperlinks doi-in-hyperlinks
         :analysis/doi-in-meta-tags doi-in-meta-tags})))


(defn analyze-all
  []
  (elastic/ensure-indexes)
  (let [counter (atom 0)]
  (elastic/every-item
    "snapshot"
    10
    (fn [result]
      ; Use the ID of the snapshot as the ID of the analysis. This means it's awlays 1-1.
      ; Also means we can do an update of all analysis, replacing each one individually, without deleting it all first.
      (let [snapshot-id (:_id result)
            snapshot (:_source result) ; Unwrap the ElasticSearch document.
            content-path (path-for-snapshot snapshot)
            content (store/get-string @snapshot-store content-path)
            analysis (analyze snapshot content)]
      
      (elastic/insert-analysis snapshot-id analysis false)
      (let [v (swap! counter inc)]
        (log/info "Analyzed so far..." v)))))))

(defn analyze-one
  [id snapshot content]
  (let [analysis (analyze snapshot content)]
    (elastic/insert-analysis id analysis false)))

(defn snapshots-chunk
  "Retrieve and process a chunk of snapshots, return the number of items processed.
   Also perform the analysis as we have the content to hand."
  []
  (elastic/ensure-indexes)
  
  (let [pids (elastic/unsnapshotted-pids)]
    (log/info "Snapshotting" (count pids) "PIDs...")
    (cp/pdoseq parallelism [pid-doc pids]
      (log/info "Snapshot for " (-> pid-doc :_source :pid/doi))
      
      (let [pid-obj (:_source pid-doc)
            pid-id (:_id pid-doc)
            [snapshot content] (snapshot-http-kit/snapshot pid-obj)
            ; Inherit fields.
            snapshot (merge pid-obj snapshot)
            snapshot (assoc snapshot :snapshot/last-analysis (System/currentTimeMillis))]

        (log/info "Snapshot:" (:snapshot/content-hash snapshot))
        
        ; Save both the content hash and the snapshot data, in case we want it later.
        (when content
          (store/set-string @snapshot-store (path-for-snapshot snapshot) content))

        (store/set-string @snapshot-store (str (path-for-snapshot snapshot) ".json") (json/write-str snapshot :indent true))
        (elastic/insert-pid-snapshot pid-id snapshot)
        (analyze-one pid-id snapshot content)))

    (count pids)))

(defn snapshots
  []
  ; As long as there are more to process, keep going.
  (loop []
    (when (> (snapshots-chunk) 0)
      (Thread/sleep 1000)
      (recur))))

(defn snapshots-forever
  []
  (loop []
    (log/info "Tick snapshots")
    (snapshots-chunk)
    (log/info "Done chunk")
    (Thread/sleep 1000)
    (recur)))

(defn all-domains
  "All domains that any DOI redirected to."
  []
  (let [domains (elastic/facet-count
                  "analysis"
                  :snapshot/final-url-domain nil)]
    (doseq [[domain cnt] domains]
      (prn domains))))


(defn crossref-domains
  "All domains that any Crossref DOI redirected to."
  []
  (let [domains (elastic/facet-count
                  "analysis"
                  :snapshot/final-url-domain
                  {:bool {:must [{:term {:pid/ra "crossref"}}]}})]
    ; Discard count.
    (map first domains)))

(defn datacite-domains
  "All domains that any DataCite DOI redirected to."
  []
  (let [domains (elastic/facet-count
                  "analysis"
                  :snapshot/final-url-domain
                  {:bool {:must [{:term {:pid/ra "datacite"}}]}})]
    ; Discard count.
    (map first domains)))

(defn meta-tag-roundtrip-ok
  "All domains that correctly and unambigiously had a DOI meta tag."
  []
  (let [domains (elastic/facet-count
                  "analysis"
                  :snapshot/final-url-domain
                  {:bool {:must [{:term {:analysis/meta-tag-correctness "correct"}}]}})]
    domains))

(defn report-for-member
  [global-member-id]
  (let [num-snapshots (elastic/query-count "analysis" {:bool {:must [{:term {:pid/global-member-id global-member-id}}]}})

        ; What's the coverage of meta tags?
        ; Result is "correct", "incorrect", "missing", "conflict". 
        meta-tag-correctness-facets (elastic/facet-count
                                      "analysis"
                                      :analysis/meta-tag-correctness
                                      {:bool {:must [{:term {:pid/global-member-id global-member-id}}]}})

        ; Domains for this member that have good meta tags.
        good-meta-tags-domains (elastic/facet-count
                                      "analysis"
                                      :snapshot/final-url-domain
                                      {:bool {:must [{:term {:pid/global-member-id global-member-id}}
                                                     {:term {:analysis/meta-tag-correctness "correct"}}]}})

        ; Domains for this member that have any kind of bad meta tags.
        bad-meta-tags-domains (elastic/facet-count
                                    "analysis"
                                    :snapshot/final-url-domain
                                    {:bool {:must [{:term {:pid/global-member-id global-member-id}}
                                                   {:bool {:should [{:term {:analysis/meta-tag-correctness "conflict"}}
                                                                    {:term {:analysis/meta-tag-correctness "incorrect"}}
                                                                    {:term {:analysis/meta-tag-correctness "missing"}}]}}]}})

        ; Future: display guidelines 
        ]
      {:num-snapshots num-snapshots
       :meta-tag-correctness-facets meta-tag-correctness-facets
       :good-meta-tags-domains good-meta-tags-domains
       :bad-meta-tags-domains bad-meta-tags-domains}))

(defn member-reports-all
  []
  (elastic/ensure-indexes)
  (let [counter (atom 0)
        member-ids (elastic/facet-count "analysis" "pid/global-member-id" nil)
        ; To pairs of [id report]
        reports (cp/pmap parallelism
                  (fn [[member-id _]]
                    (swap! counter inc)
                    (when (zero? (rem @counter 1000))
                      (log/info "Done" @counter))
                    [member-id {:report (report-for-member member-id)}])
                  member-ids)]

    (elastic/insert-docs-ids "member-report" reports))
  (log/info "Done reports!"))

(defn report-for-prefix
  [prefix]
  (let [good-meta-tags-domains (elastic/facet-count
                                 "analysis"
                                 :snapshot/final-url-domain
                                 {:bool {:must [{:term {:pid/doi-prefix prefix}}
                                                {:term {:analysis/meta-tag-correctness "correct"}}]}})
        doi-in-hyperlinks-domains (elastic/facet-count
                               "analysis"
                               :snapshot/final-url-domain
                               {:bool {:must [{:term
                                                {:pid/doi-prefix prefix}}
                                              {:bool
                                                {:should
                                                  [{:term {:analysis/doi-in-hyperlinks "https-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "https-dx-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "http-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "https-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "http-dx-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "doi-scheme"}}
                                                   {:term {:analysis/doi-in-hyperlinks "non-url"}}]}}]}})
        doi-in-text-domains (elastic/facet-count
                               "analysis"
                               :snapshot/final-url-domain
                               {:bool {:must [{:term
                                                {:pid/doi-prefix prefix}}
                                              {:bool
                                                {:should
                                                  [{:term {:analysis/doi-in-text "https-doi"}}
                                                   {:term {:analysis/doi-in-text "https-dx-doi"}}
                                                   {:term {:analysis/doi-in-text "http-doi"}}
                                                   {:term {:analysis/doi-in-text "https-doi"}}
                                                   {:term {:analysis/doi-in-text "http-dx-doi"}}
                                                   {:term {:analysis/doi-in-text "doi-scheme"}}
                                                   {:term {:analysis/doi-in-text "non-url"}}]}}]}})

        good-meta-tags-domains (map first good-meta-tags-domains)
        doi-in-text-domains (map first doi-in-text-domains)
        doi-in-hyperlinks-domains (map first doi-in-hyperlinks-domains)

        good-domains (set (concat good-meta-tags-domains doi-in-text-domains doi-in-hyperlinks-domains))]

        {:good-meta-tags-domains good-meta-tags-domains
         :doi-in-text-domains doi-in-text-domains
         :doi-in-hyperlinks-domains doi-in-hyperlinks-domains
         :good-domains good-domains}))

(defn report-for-domain
  [domain]
  (let [good-meta-tags-prefixes (elastic/facet-count
                                 "analysis"
                                 :pid/doi-prefix
                                 {:bool {:must [{:term {:snapshot/final-url-domain domain}}
                                                {:term {:analysis/meta-tag-correctness "correct"}}]}})
        doi-in-hyperlinks-prefixes (elastic/facet-count
                               "analysis"
                               :pid/doi-prefix
                               {:bool {:must [{:term
                                                {:snapshot/final-url-domain domain}}
                                              {:bool
                                                {:should
                                                  [{:term {:analysis/doi-in-hyperlinks "https-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "https-dx-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "http-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "https-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "http-dx-doi"}}
                                                   {:term {:analysis/doi-in-hyperlinks "doi-scheme"}}
                                                   {:term {:analysis/doi-in-hyperlinks "non-url"}}]}}]}})
        doi-in-text-prefixes (elastic/facet-count
                               "analysis"
                               :pid/doi-prefix
                               {:bool {:must [{:term
                                                {:snapshot/final-url-domain domain}}
                                              {:bool
                                                {:should
                                                  [{:term {:analysis/doi-in-text "https-doi"}}
                                                   {:term {:analysis/doi-in-text "https-dx-doi"}}
                                                   {:term {:analysis/doi-in-text "http-doi"}}
                                                   {:term {:analysis/doi-in-text "https-doi"}}
                                                   {:term {:analysis/doi-in-text "http-dx-doi"}}
                                                   {:term {:analysis/doi-in-text "doi-scheme"}}
                                                   {:term {:analysis/doi-in-text "non-url"}}]}}]}})

        ; Prefixes that failed to match for this domain.
        bad-prefixes (elastic/facet-count
                               "analysis"
                               :pid/doi-prefix
                               {:bool {:must [{:term {:snapshot/final-url-domain domain}}
                                              {:bool {:must_not {:exists {:field :analysis/doi-in-text}}}}
                                              {:bool {:must_not {:exists {:field :analysis/doi-in-hyperlinks}}}}
                                              {:bool {:must_not {:exists {:field :analysis/doi-in-meta-tags}}}}]}})

        good-meta-tags-prefixes (map first good-meta-tags-prefixes)
        doi-in-text-prefixes (map first doi-in-text-prefixes)
        doi-in-hyperlinks-prefixes (map first doi-in-hyperlinks-prefixes)
        bad-prefixes (map first bad-prefixes)

        good-prefixes (set (concat good-meta-tags-prefixes doi-in-text-prefixes doi-in-hyperlinks-prefixes))]

        {:good-meta-tags-prefixes good-meta-tags-prefixes
         :doi-in-text-prefixes doi-in-text-prefixes
         :doi-in-hyperlinks-prefixes doi-in-hyperlinks-prefixes
         :good-prefixes good-prefixes
         :bad-prefixes bad-prefixes}))

(defn prefix-reports-for-ra
  [ra]
  (let [prefixes (elastic/facet-count "analysis" "pid/doi-prefix" {:term {:pid/ra ra}})
        ; To pairs of [id report]
        reports (cp/pmap parallelism
                  (fn [[prefix _]]
                    [prefix {:report (assoc (report-for-prefix prefix) :ra ra)}])
                  prefixes)]
    (elastic/insert-docs-ids "prefix-report" reports))
  (log/info "Done reports!"))

(defn prefix-reports-all
  []
  (elastic/ensure-indexes)
  (prefix-reports-for-ra "crossref")
  (prefix-reports-for-ra "datacite"))

(defn domain-reports-for-ra
  [ra]
  (elastic/ensure-indexes)
  (let [domains (elastic/facet-count "analysis" "snapshot/final-url-domain" {:term {:pid/ra ra}})
        ; To pairs of [id report]
        reports (cp/pmap parallelism
                  (fn [[domain _]]
                    [domain {:report (assoc (report-for-domain domain) :ra ra)}])
                  domains)]
    (elastic/insert-docs-ids "domain-report" reports))
  (log/info "Done reports!"))

(defn domain-reports-all
  []
  (elastic/ensure-indexes)
  (domain-reports-for-ra "crossref")
  (domain-reports-for-ra "datacite"))


(defn generate-artifact-good-prefixes-domain
  "Generate the prefixes-domain artifact. 
   These are all pairs of prefixes and domains, possily many-to-many, where the DOI is contained in the page somehow."
  []
  (with-open [output (writer "prefixes-domains")]
    (elastic/every-item
      "prefix-report"
      1
      (fn [report]
        (let [prefix (-> report :_id)
              ra (-> report :_source :report  :ra)
              good-domains (-> report :_source :report  :good-domains)]
          
          ; Explicitly list prefixes with no domains.
          (when (empty? good-domains)
            (.write output (str prefix ",," ra "\n")))

          (doseq [domain good-domains]
            (.write output (str prefix "," domain "," ra "\n"))))))))

(defn generate-artifact-bad-prefixes-domain
  "Generate the bad-prefixes-domain artifact.
   These are all pairs of prefixes and domains, possily many-to-many, where the DOI is not contained in the page.
   This is basically all the domains we arrived at that didn't have a link back to the DOI."
  []
  (with-open [output (writer "prefixes-domains-bad")]
      (elastic/every-item
        "domain-report"
        1
        (fn [report]

          ; (prn "REPORT" report)
          (let [domain (-> report :_id)
                ra (-> report :_source :report  :ra)
                bad-prefixes (-> report :_source :report  :bad-prefixes)]

            (doseq [prefix bad-prefixes]
              (.write output (str prefix "," domain "," ra "\n"))))))))

; Main functions in order of data flow.

(defn main-sample-members
  "Take a representative sample of DOIs from all members."
  []
  (sample-members))

(defn main-snapshots
  "Snapshot all un-snapshotted PIDs, and perform analysis."
  []
  (snapshots))

(defn main-re-analyze
  "Reprocess all snapshots to perform analysis."
  []
  (analyze-all))

(defn main-artifacts
  "Given an up to date 'analysis' index, generate prefix and domain reports, then artifacts."
  []
  (domain-reports-all)
  (prefix-reports-all)
  (generate-artifact-good-prefixes-domain)
  (generate-artifact-bad-prefixes-domain))

(defn -main
  [& args])
  