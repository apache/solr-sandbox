# Solr-Community-Datasets

Utility scripts for fetching datasets related to the Solr community, and preparing them for ingestion into Solr.
All created documents rely on dynamic field suffixes, and should work with Solr's `_default` configset.

## Mailing List Data

Run the following to download and prepare mailing list data for Solr ingestion:

```
export MBOX_OUTPUT_DIR="/fill/in/with/dir"
export SOLR_DOC_OUTPUT_DIR="/fill/in/with/other/dir"
./download-mailing-lists.sh $MBOX_OUTPUT_DIR
./convert-mailing-lists-to-solr-docs.sh $MBOX_OUTPUT_DIR $SOLR_DOC_OUTPUT_DIR
```

Currently, the created documents reflect email metadata only.
Email content itself isn't captured for search, though nothing precludes that if users wish to make the requisite changes to `convert-mbox-to-solr-docs.py`.

### Example Mailing List Queries

Assuming mailing list traffic ingested into a collection `maildata`, it supports the following example queries:

**Human List Traffic by Month**

```
export COLLECTION="maildata"
curl -sk "http://localhost:8983/solr/$COLLECTION/select?facet.field=date_bucket_month_s&\
facet.sort=index&\
facet=true&\
indent=true&\
q=list_s:dev+OR+list_s:users&\
rows=0"
```

**Human List Traffic by Fiscal Quarter** (useful for board-reports)

```
export COLLECTION="maildata"
curl -sk "http://localhost:8983/solr/$COLLECTION/select?facet.field=date_bucket_quarter_s&\
facet.sort=index&\
facet=true&\
indent=true&\
q=list_s:dev+OR+list_s:users&\
rows=0"
```

## Git Commit Data

Run the snippet below to download and prepare git-commit data for Solr ingestion.
Preparing git data can take a good bit longer than other sources described here, so consider a coffee while it runs.

```
export COMMIT_OUTPUT_DIR="/fill/in/with/dir"
export SOLR_DOC_OUTPUT_DIR="/fill/in/with/other/dir"
./download-git-repositories.sh $COMMIT_OUTPUT_DIR
./convert-git-repositories-to-solr-docs.sh $COMMIT_OUTPUT_DIR $SOLR_DOC_OUTPUT_DIR
```

### Example Git Data Queries

Assuming git-data ingested into a collection `gitdata`, it supports the following example queries:

**Compare Commit Volume b/w Two Fiscal Quarters**

Fiscal "quarters" aren't currently computed at index time, as with the mailing list data above, but users can still achieve a similar affect by specifying quarters of interest.
The query below compares Q1 FY2025 (May-July 2024) with Q1 FY2024 (May-June 2023):

```
export COLLECTION="git_data"
curl -sk "http://localhost:8983/solr/$COLLECTION/select" -d '
{
  "query": "*:*",
  "limit": 0,
  "facet": {
    "q1_fy2025": {
      "type": "query",
      "q": "date_dt:[2024-05-01T00:00:00Z TO 2024-08-01T00:00:00Z]"
    },
    "q1_fy2024": {
      "type": "query",
      "q": "date_dt:[2023-05-01T00:00:00Z TO 2023-08-01T00:00:00Z]"
    }
  }
}'
```
