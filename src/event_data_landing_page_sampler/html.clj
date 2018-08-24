(ns event-data-landing-page-sampler.html
  (:require [clojure.tools.logging :as log]
            [crossref.util.doi :as cr-doi]
            [clojure.string :as string])
  (:import [org.jsoup Jsoup]))


(defn doi-expressions
  "Take a DOI and return a list of ways the DOI can be expressed, in order of preference.
   Result is a sequence of [form form-type]. Expressions are always lower-cased."
  [doi]
  (let [doi (clojure.string/lower-case (cr-doi/non-url-doi doi))
        
        ; Sometimes DOIs contain encodable characters, e.g. SICIs. Check for those too.
        encoded (java.net.URLEncoder/encode doi "UTF-8")

        https-dx-doi (str "https://dx.doi.org/" doi)
        https-doi (str "https://doi.org/" doi)
        http-dx-doi (str "http://dx.doi.org/" doi)
        http-doi (str "http://doi.org/" doi)

        https-dx-doi-encoded (str "https://dx.doi.org/" encoded)
        https-doi-encoded (str "https://doi.org/" encoded)
        http-dx-doi-encoded (str "http://dx.doi.org/" encoded)
        http-doi-encoded (str "http://doi.org/" encoded)

        ; Getting into the weeds but complete coverage is useful.
        doi-scheme (str "doi:" doi)
        doi-scheme-encoded (str "doi:" encoded)]
    
    ; This order is a combination of preference and substrings, as these are used for searching text.
    ; The ordering between mutually exclusive patterns matters becuase there may be two forms in
    ;  the same text, and we want to identify the most preferable.
    [[https-doi :https-doi]
     [https-doi-encoded :https-doi]
     [https-dx-doi :https-dx-doi]
     [https-dx-doi-encoded :https-dx-doi]

     [http-doi :http-doi]
     [http-doi-encoded :http-doi]
     [http-dx-doi :http-dx-doi]
     [http-dx-doi-encoded :http-dx-doi]

     [doi-scheme :doi-scheme]
     [doi-scheme-encoded :doi-scheme]

     ; These come last as they're factors of all other strings.
     [encoded :non-url]
     [doi :non-url]]))

(def interested-meta-tag-names
  "Case insensitive. This is a bit stricture than the Percolator will accept, as we're trying to find the domains that do it correctly."
  #{"citation_doi"
    "dc.identifier"
    "dc.identifier.doi"})

(defn identifier-doi-tags-present
  "Return seq of [meta-tag-name, content] for all types of meta tag we're interested in."
  [html-text]
  (try
    (when html-text
      (let [document (Jsoup/parse html-text)

            ; Get specific attribute values from named elements.
            ; There may be more than one per type!
            ; Only take those that have well-formed DOIs to exclude competing identifiers.
            attr-values (mapcat (fn [meta-tag-name]
                                  (->>
                                    (.select document (str "meta[name=" meta-tag-name "]"))
                                    (map #(vector meta-tag-name (.attr % "content")))))
                                interested-meta-tag-names)]

        attr-values))
    ; We're getting text from anywhere. Anything could happen.
    (catch Exception ex (do
      (log/warn "Error parsing HTML for DOI.")
      (.printStackTrace ex)
      nil))))

(defn unambiguous-doi
  "Take a selection of [tag-name content]. Return nil if there's nothing there, :ambigious if there are clashing DOIs, or the DOI"
  [input]
  (try
    (let [unique-vals (some->> input
                               (map second)
                               (filter cr-doi/well-formed)
                               ; If it's obviously not a DOI, then ignore.
                               (map cr-doi/non-url-doi)
                               (map clojure.string/lower-case)
                               set)]
      (cond (empty? unique-vals) nil
            (> (count unique-vals) 1) "ambiguous"
            (= (count unique-vals) 1) (first unique-vals)))

      ; This can happen when we get really weird inputs.
      ; Record these rather than just return nil.
      (catch Exception _ "bad")))


(defn plaintext-from-html
  "Extract a single plaintext string from text of whole document."
  [html]
  (try
    (-> html
        Jsoup/parse
        (.body)
        (.text))
    ; We're getting text from anywhere. Anything could happen.
    (catch Exception ex (do
      (log/warn "Error parsing HTML for text")
      ""))))

(defn extract-a-hrefs-from-html
  "Return set of link hrefs converted to lower case (or empty set)."
  [input]
  (let [links (-> (or input "")
          Jsoup/parse
          (.select "a[href]"))
        hrefs (keep #(.attr % "href") links)
        hrefs (map clojure.string/lower-case hrefs)]
    (set hrefs)))

(defn doi-in-html-text
  "Which version of the DOI, if any, is found in the HTML text?"
  [doi html]
  (let [text (plaintext-from-html html)
        text (clojure.string/lower-case text)
        expressions (doi-expressions doi)]
    (first (keep (fn [[expression label]] (when (.contains text expression) label)) expressions))))

(defn doi-in-hyperlinks
  "Which version of the DOI, if any, is found in hyperlinks?"
  [doi html]
  (let [links (extract-a-hrefs-from-html html)
        expressions (doi-expressions doi)]
    (first (keep (fn [[expression label]] (when (links expression) label)) expressions))))

(defn doi-in-meta-tags
  "Which version of the DOI, if any, is found in the meta tags?"
  [doi html]
  (let [expressions (doi-expressions doi)
        meta-tags (identifier-doi-tags-present html)
        meta-tag-values (map second meta-tags )
        meta-tag-values (map clojure.string/lower-case meta-tag-values)
        meta-tag-value-set (set meta-tag-values)]
    (first (keep (fn [[expression label]] (when (meta-tag-value-set expression) label)) expressions))))

