#!/bin/bash -x

# Usage: ./export-git-data.sh <repository-path> <solr-doc-output-dir>

if [[ -z ${1:-} ]]; then
  echo "'repository-path' argument is required but was not provided; exiting"
  exit 1
fi
if [[ -z ${2:-} ]]; then
  echo "'solr-doc-output-dir' argument is required but was not provided; exiting"
  exit 1
fi

REPOSITORY_PATH=$1
REPO=$(basename $REPOSITORY_PATH)
SOLR_DOC_OUTPUT_DIRECTORY="$2"

INT_SOLR_DOC_FILE="${REPO}-int-commit-data.json"
FIELD_SEPARATOR="|||"
MY_CWD=$(pwd)
TMP_FILE="${MY_CWD}/raw-git-data.txt"
# Uncomment to restrict commits to a particular directory
#IN_REPO_PATH="solr/"

pushd $REPOSITORY_PATH
  git log --format=format:"%h$FIELD_SEPARATOR%an$FIELD_SEPARATOR%ad$FIELD_SEPARATOR%s" --date=iso8601-strict ${IN_REPO_PATH:-} > $TMP_FILE
popd

while IFS= read -r line
do
  echo "Reading line $line"
  hash_field=$(echo "$line" | awk -F'\\|\\|\\|' '{print $1}')
  name_field=$(echo "$line" | awk -F'\\|\\|\\|' '{print $2}')
  date_field=$(echo "$line" | awk -F'\\|\\|\\|' '{print $3}')
  subject_field=$(echo "$line" | awk -F'\\|\\|\\|' '{print $4}' | tr -d "[:cntrl:]" | sed 's/\\//g' | sed 's/\"/\\\"/g')

  echo "{\"id\": \"$hash_field\", \"name_s\": \"$name_field\", \"date_dt\": \"$date_field\", \"subject_s\": \"$subject_field\", \"subject_txt\": \"$subject_field\", repo_s:\"$REPO\"}," >> $INT_SOLR_DOC_FILE
done < "$TMP_FILE"
rm "$TMP_FILE"

# All the lines exist now, they just need formatted into an array
FINAL_SOLR_DOC_FILE="${SOLR_DOC_OUTPUT_DIRECTORY}/${REPO}-commit-data.json"
echo "[" > $FINAL_SOLR_DOC_FILE
cat $INT_SOLR_DOC_FILE | sed '$ s/.$//' >> $FINAL_SOLR_DOC_FILE
echo "]" >> $FINAL_SOLR_DOC_FILE
rm $INT_SOLR_DOC_FILE

echo "Git data exported to $FINAL_SOLR_DOC_FILE as JSON docs; ready for indexing and analysis"
