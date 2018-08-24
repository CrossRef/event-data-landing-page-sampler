# Event Data Landing Page Sampler

Sample DOIs from Crossref and DataCite and analyze their landing pages.

Produce the "doi-domain" Artifact for Event Data to help Agents search and identify DOIs.

## Data Collection and Storage

The following entities are stored in Elasticsearch, as they pass through the pipeline. Each stage's entries are a superset of the previous stage, and all keys are namespaced. This denormalisation makes the process fit the non-join model nicely, and preserves the original values.

### Persistent Identifier

A DOI as an identifier (not the work it may refer to). Stored in normalised non-URL form. These are sampled from the Crossref and DataCite APIs, with the sample size in proportion to the number of works a given member has.

Fields:

 - _id - Normalized DOI
 - pid/timestamp - ISO8601 timestamp of when DOI was sampled.
 - pid/doi - Normalized DOI
 - pid/doi-prefix - Prefix of the DOI (not the owner prefix).
 - pid/global-member-id - Member ID as "«ra»/«internal id»"
 - pid/member-id - Member ID within the RA.
 - pid/ra - Registration Agency, e.g. "crossref" or "datacite".

### Snapshot

The result of visiting a DOI's URL. This is currently done using an HTTP library, but in future an additional driver based on a web browser may be used.

Fields

 - _id - Auto-generated.
 - pid/doi
 - pid/doi-prefix - Prefix of the DOI (not the owner prefix).
 - pid/global-member-id - Member ID as "«ra»/«internal id»"
 - pid/member-id - Member ID within the RA.
 - pid/ra - Registration Agency, e.g. "crossref" or "datacite".
 - pid/timestamp
 - snapshot/resource-url - The URL returned by the Handle server at the time of indexing.
 - snapshot/timestamp - ISO8601 timestamp of when snapshot was taken.
 - snapshot/content-hash - Hash of returned content. The full retrieved content is stored in S3 using the hash as an ID.
 - snapshot/urls - Array of URLs that were visited as a result of redirects.
 - snapshot/final-url-domain
 - snapshot/final-url - The URL from which the content was retrieved.
 - snapshot/driver - The method used to retrieve the snapshot. Currently always "http", but may be expanded to "browser" in future.

### Analysis

Analysis performed on a Snapshot. Because we may want to add features in future, this process is decoupled from the snapshot-saving process.

Fields

 - _id - Auto-generated.
 - pid/doi
 - pid/doi-prefix
 - pid/global-member-id
 - pid/member-id
 - pid/ra
 - pid/resource-url
 - pid/timestamp
 - snapshot/timestamp
 - snapshot/content-hash
 - snapshot/urls
 - snapshot/final-url
 - snapshot/final-url-domain
 - snapshot/driver
 - analysis/meta-tags - A sequece of meta tags and their values, e.g `[["citation-doi", "10.5555"], ["dc.identifier", "https://doi.org/10.5555/12345678"]]`.
 - analysis/meta-tag-correctness - One of 'missing', 'conflict', 'ok' or 'incorrect'. Is the DOI unambigiously mentioned in meta tags? Duplication is OK, as long as they have the same, correct, DOI.

## Reporting

Two types of reports are generated: per-prefix and per-domain. Each one contains information about the domains or prefixes (respectively) that were succesfully linked.

## Artifacts

Two artifacts are produced, `prefixes-domains` and `prefixes-domains-bad`. They are both in CSV format with lines of `prefix,domain,ra`.

`prefixes-domains` contains every combination of prefix and domain (can be many to many) where at least one DOI on that prefix resolved to a page on that domain, and the web page contained the DOI either in the meta tag, body text, or hyperlink. If a prefix had no succesful domains, then an empty field will be recorded, e.g. `10.5555,,crossref`. This means that all known prefixes are recorded.

`prefixes-domains` is the same format, but it contains entries where there was at least one DOI with the given prefix that resolved to a webpage on that domain, but the DOI was not recorded on that page. Every known domain followed from a DOI is recorded.

Because there can be inconsistency, the two lists may overlap.

## Coverage

This needs to be run often, probably continuously, to be kept up to date. The list is meant to be additive, meaning that it could contain entries that are no longer true, but are useful for processing the historical record.

