# -*- coding: utf-8 -*-

import requests
import concurrent.futures
import pandas as pd

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from google.colab import drive
import os
import time

import math
import json

import pprint as pp
import csv

pp = pp.PrettyPrinter(indent=4)

# Mount Google Drive
drive.mount('/content/drive')
SAVE_PATH = "/content/drive/My Drive/CrossRef_ROR/"
os.makedirs(SAVE_PATH, exist_ok=True)

# API Configurations

# Crossref
BASE_URL = "https://api.crossref.org/works"
FILTERS = "type:journal-article,has-ror-id:true,from-pub-date:2024-10-01,until-pub-date:2024-12-31"
POLITE = "aravindvenkatesan@gmail.com"
ROWS = 1000  # Maximum number of records per API call
TOTAL_RESULTS = 13322#12406#61834  # Known total results from API

"""
Fetches affiliation data from CrossRef API and extracts unique ROR IDs.

Args: BASE_URL - Base URL for the CrossRef API.
      FILTERS - Filters to be applied to the API request.
      POLITE - Email address for polite contact.
      ROWS - Number of records per API call.
      TOTAL_RESULTS - # Known total results from API

"""

def fetch_affiliation_data(BASE_URL, FILTERS, POLITE, ROWS, TOTAL_RESULTS):

    records = {}
    unique_ror_ids = set()
    cursor = "*"
    counter = 0
    num_calls = math.ceil(TOTAL_RESULTS / ROWS)
    print(f"---- Calls to be made: {num_calls}")
    while counter <= num_calls:

        url = f"{BASE_URL}?filter={FILTERS}&rows={ROWS}&cursor={cursor}&mailto={POLITE}"

        try:
          response = requests.get(url, timeout=10)
          response.raise_for_status()
          data = response.json()

          if "message" not in data or "items" not in data["message"]:
            print("Warning: Unexpected API response format.")
            break

          for item in data["message"].get("items", []):
            try:
              if 'author' in item:
                doi = item.get('DOI')
                # print(doi)
                for author in item['author']:
                  if 'affiliation' in author:

                    aff_names = list()

                    for aff in author['affiliation']:
                      if 'name' in aff:
                        aff_names.append(aff['name'])

                      if 'acronym' in aff and isinstance(aff['acronym'], list):
                        for acronym in aff['acronym']:
                          aff_names.append(acronym)

                      if doi not in records:
                        records[doi] = {}
                      if doi in records:
                        inner_obj = {
                                  'ROR ID': None,
                                  'Affiliation Names': []
                                  }

                        inner_obj['Affiliation Names'].extend(aff_names)

                        if 'id' in aff:
                          for details in aff['id']:
                            if details.get('id-type') == 'ROR':
                              ror_id = details.get('id', 'Unknown').replace('https://ror.org/', '')
                              # print(ror_id)
                              inner_obj['ROR ID'] = ror_id

                              unique_ror_ids.add(ror_id)

                              records[doi].update(inner_obj)

                  else:
                    if doi not in records:
                      records[doi] = {}

            except KeyError as ke:
              print(f"Key error processing item: {ke}")
            except Exception as e:
              print(f"Unexpected error processing an item: {e}")

          new_cursor = data["message"].get("next-cursor")
          cursor = new_cursor
          counter += 1

          time.sleep(20)
          call_message = f"\rNo. of calls completed: {counter}/{num_calls}"
          print(call_message, end="")

        except requests.exceptions.RequestException as e:
          print(f"Error fetching data: {e}")

        finally:
          requests.session().close()

    return records, unique_ror_ids

"""
  Opens two files, a list of ROR IDs and the ROR metadata file (in JSON format).
  Args:
        ror_id_file: Path to the file containing a list of ROR IDs.
        json_file: Path to the large JSON file with metadata.
        output_file: Path to the file to write the output to.
"""
def process_ror_data_from_file(ror_id_file, json_file, output_file):


    try:
        ror_dict = {}
        with open(ror_id_file, 'r') as f_ids, open(json_file, 'r') as f_json, open(output_file, 'wt') as writer:
            ror_ids = [line.strip() for line in f_ids]

            json_data = f_json.read()
            objects = json.loads(json_data)

            writer.write(f"ROR ID\tLabel\n")

            for ror_id in ror_ids:
              ror_uri = f"https://ror.org/{ror_id}"

              for obj in objects:

                    if 'id' in obj and obj['id'] == ror_uri:
                        # Process the data for the matching ROR ID

                          message = f"\rProcessing ROR ID: {ror_id}"
                          print(message, end="")

                          if 'name' in obj:
                            writer.write(f"{ror_id}\t{obj['name']}\n")


                          if len(obj.get("labels")) >= 1:
                            for label in obj['labels']:

                              writer.write(f"{ror_id}\t{label['label']}\n")

                          if len(obj.get("acronyms")) >= 1:
                            for acronym in obj['acronyms']:

                              writer.write(f"{ror_id}\t{acronym}\n")

                          if len(obj.get("aliases")) >= 1:
                            for alias in obj['aliases']:
                              writer.write(f"{ror_id}\t{alias}\n")

                    else:
                      continue

            print("Finished processing data for all provided ROR IDs.")

    except FileNotFoundError:
        print(f"Error: One or both files not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

"""
 Matches CrossRef affiliations to ROR names
 Args:
        crossref_data: Data structure containing CrossRef affiliation data.
        ror_data: Data structure containing ROR names.

"""
def match_affiliations_to_ror(crossref_data, ror_data):

    matched_results = []

    for doi, affiliations in crossref_data.items():
        ror_id = affiliations['ROR ID']
        for aff in affiliations['Affiliation Name']:
            crossref_name = aff
            if ror_id in ror_data:
              ror_info = ror_data[ror_id]
              if crossref_name in ror_info:

                matches = process.extractWithoutOrder(crossref_name, ror_info, scorer=fuzz.partial_token_set_ratio, score_cutoff=20)

                for mt in matches:

                  if mt:
                    selected = mt[0]
                    score = mt[1]
                    if score >= 90:
                        match_status = "High Confidence Match"
                    elif 80 <= score < 90:
                        match_status = "Moderate Confidence Match"
                    elif 70 <= score < 80:
                        match_status = "Low Confidence Match"
                    else:
                        match_status = "No Reliable Match"

                  matched_results.append({
                      "DOI": doi,
                      "Crossref affiliation": crossref_name,
                      "Matched ROR name": selected,
                      "ROR ID": ror_id,
                      "Match score": score,
                      "Match status": match_status
                      })

    print(f"Matching complete. Results saved to {save_path}")
    return matched_results

def main():
    """
    Main function to run different data processing tasks.
    """

    while True:
        print("\nChoose an option:")
        print("1. Fetch affiliation data")
        print("2. Process ROR data from file")
        print("3. Match affiliations to ROR")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            # Parameters for fetch_affiliation_data
            base_url = input("Enter the base url (leave empty for default): ") or BASE_URL
            filters = input("Enter the filters (leave empty for default): ") or FILTERS
            polite = input("Enter the polite email (leave empty for default): ") or POLITE
            rows = int(input("Enter rows (leave empty for default): ") or ROWS)
            total_results = int(input("Enter total results (leave empty for default): ") or TOTAL_RESULTS)

            fetch_affiliation_data(base_url, filters, polite, rows, total_results)

        elif choice == '2':
            ror_id_file = input("Enter the path to the ROR ID file: ")
            json_file = input("Enter the path to the JSON file: ")
            output_file = input("Enter the path to the output file: ")
            process_ror_data_from_file(ror_id_file, json_file, output_file)

        elif choice == '3':
            crossref_data = input("Enter the path to the crossref data file: ")
            ror_data = input("Enter the path to the ror data file: ")
            save_path = input("Enter the path to save the matched affiliations file: ")
            match_affiliations_to_ror(crossref_data, ror_data, save_path)

        elif choice == '4':
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()