#!/usr/local/bin/python3

import mailbox
import sys
import os
import uuid
import json
from datetime import datetime

# Potential Improvements:
#  - more cleaning for 'subject' and other free text fields
#  - some regex parsing to separate sender name/email in 'from' fields
#  - capture other fields
def convert_message_to_solr_doc(message, source_list):
    solr_doc = dict()
    solr_doc["id"] = str(uuid.uuid4())
    solr_doc["from_s"] = message.get("From")
    solr_doc["list_s"] = source_list

    # List-Id, for whatever reason, is always 'dev.solr.apache.org', so omitting this for now
    #if "List-Id" in message:
    #    solr_doc["mailing_list_s"] = message["List-Id"]

    # 'To' might contain multiple addresses, separated by commas
    sender_unsplit = message.get("To")
    senders = [line.strip() for line in sender_unsplit.split(",")]
    solr_doc["to_s"] = sender_unsplit
    solr_doc["to_ss"] = senders

    # Solr requires dates in a particular format
    date_str_raw = message.get("Date").replace("(MST)", "").replace("(UTC)", "").replace("(CST)", "").replace("(EST)", "").strip()
    try:
        date_obj = datetime.strptime(date_str_raw, "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        date_obj = datetime.strptime(date_str_raw, "%d %b %Y %H:%M:%S %z")
    solr_doc["sent_dt"] = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    solr_doc["date_bucket_month_s"] = to_monthly_bucket(date_obj)
    solr_doc["date_bucket_quarter_s"] = to_quarterly_bucket(date_obj)

    subject_raw = message.get("Subject")
    subject_cleaned = subject_raw.lower()
    if subject_cleaned.startswith("re: "):
        subject_cleaned = subject_cleaned.replace("re: ", "", 1)
    solr_doc["subject_raw_s"] = subject_raw
    solr_doc["subject_raw_txt"] = subject_raw
    solr_doc["subject_clean_s"] = subject_cleaned
    solr_doc["subject_clean_txt"] = subject_cleaned

    return solr_doc

def to_monthly_bucket(date_obj):
    padded_month = str(date_obj.month)
    if len(padded_month) == 1:
        padded_month = "0" + padded_month
    return str(date_obj.year) + "-" + padded_month

# Returns a string representing the ASF fiscal quarter this email was sent in. (Useful for compiling quarterly reports!)
# ASF Fiscal quarters are a bit odd.  I don't understand them.  But the logic appears to be, taking FY2020 as an example:
#   - Q1 of FY2020 is May, June, and July of 2019
#   - Q2 of FY2020 is August, September, October of 2019
#   - Q3 of FY2020 is November and December of 2019, and January of 2020
#   - Q4 of FY2020 is February, March, and April of 2020
# Why would "Q1" start in May?  Why would the FY and the calendar year be offset in this manner? :shrug:
def to_quarterly_bucket(date_obj):
    month = date_obj.month
    year = date_obj.year

    if month >= 2 and month <= 4:
        quarter = "Q4"
        fiscal_year = year
    elif month >= 5 and month <= 7:
        quarter = "Q1"
        fiscal_year = year + 1
    elif month >= 8 and month <= 10:
        quarter = "Q2"
        fiscal_year = year + 1
    else: # month = 11, 12, 1
        quarter = "Q3"
        if month == 1:
            fiscal_year = year
        else:
            fiscal_year = year + 1
    return str(year) + "-" + quarter

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Incorrect arguments provided")
        print("  Usage: convert-mbox-to-solr-docs.py <mbox-file> <output-directory>")
        sys.exit(1)

    mbox_filepath = sys.argv[1]
    output_directory = sys.argv[2]

    # Filename is assumed to be in the form sourceList-YYYY-MM.mbox (e.g. builds-2023-5.mbox)
    mbox_filename = os.path.basename(mbox_filepath)
    source_list = mbox_filename.split("-")[0]
    solr_doc_filename = mbox_filename.replace(".mbox", ".json")
    solr_doc_filepath = os.path.join(output_directory, solr_doc_filename)

    with open(solr_doc_filepath, 'w') as solr_doc_writer:
        solr_doc_writer.write("[")
        first_doc = True
        for message in mailbox.mbox(mbox_filepath):
            if not first_doc:
                solr_doc_writer.write(",")
            first_doc = False
            solr_doc_writer.write("\n")
            solr_doc = convert_message_to_solr_doc(message, source_list)
            json.dump(solr_doc, solr_doc_writer)
        solr_doc_writer.write("]")
