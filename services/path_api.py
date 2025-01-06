import os
import pandas as pd
import json
import requests
from urllib.parse import urlencode, quote, quote_plus
import logging
from datetime import datetime
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Get environment variables
PATH_API_HOST = os.getenv("PATH_API_HOST")
USERNAME = os.getenv("PATH_API_USERNAME")
PASSWORD = os.getenv("PATH_API_PASSWORD")

# Validate environment variables
if not all([PATH_API_HOST, USERNAME, PASSWORD]):
    raise ValueError(
        "Missing required environment variables. Please check your .env file."
    )


def login():
    """Logs in to the Path API and retrieves an access token."""
    login_url = f"{PATH_API_HOST}/login"
    login_data = {"username": USERNAME, "password": PASSWORD}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(
        login_url, data=urlencode(login_data), headers=headers, verify=False
    )
    if response.status_code == 200:
        token = response.json().get("token", {}).get("tokenValue")
        logger.info(f"Login successful")
        return token
    else:
        logger.error(
            f"Login failed. Status: {response.status_code}, Response: {response.text}"
        )
        raise Exception("Login failed")


def get_slide_metadata(token, filename):
    """Fetches slide metadata for the given filename."""
    safe_filename = quote(filename, safe=";")
    url = f"{PATH_API_HOST}/slides?file_name={safe_filename}.ndpi"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers, verify=False)
    logger.info(f"Requesting slide metadata with URL: {url}")
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        logger.warning(f"No metadata found for {filename}")
        return None
    else:
        logger.error(f"Error retrieving metadata for {filename}: {response.text}")
        raise Exception(f"Error retrieving metadata for {filename}")


def get_retrieval_flag_info(token, case_id):
    """Fetches retrieval flag information for a given case ID."""
    url = f"{PATH_API_HOST}/lims/accession/{case_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        retrieval_flags = response.json().get("RetrievalFlags", [])
        if retrieval_flags:
            return {"retrieval_flag": retrieval_flags[0]}
    elif response.status_code == 404:
        logger.warning(f"No retrieval flag info found for case {case_id}")
        return None
    else:
        logger.error(
            f"Error retrieving retrieval flag info for case {case_id}: {response.text}"
        )
        raise Exception(f"Error retrieving retrieval flag info for case {case_id}")


if __name__ == "__main__":

    retrieval_flag_df_dict = {
        "case_id": [],
        "slide_name": [],
        "slide_datetime": [],
        "retrieval_flag": [],
    }
    try:

        from tqdm import tqdm

        enddatetime = "2024-12-08 00:00:00"
        enddatetime = pd.to_datetime(enddatetime, format="%Y-%m-%d %H:%M:%S")

        slide_dir = "/pesgisipth/NDPI"

        # get a list of filenames in slide_dir that start with "H" and end with ".ndpi"
        slide_files = [
            f
            for f in os.listdir(slide_dir)
            if f.startswith("H") and f.endswith(".ndpi")
        ]

        def get_slide_datetime(slide_name):
            try:
                name = slide_name.split(".ndpi")[0]
                datetime = name.split(" - ")[-1]

                # convert the datetime to a datetime object
                datetime = pd.to_datetime(datetime, format="%Y-%m-%d %H.%M.%S")
            except Exception as e:
                print(f"Error getting datetime for {slide_name}: {e}")
                raise e
            return datetime

        # Get authentication token
        token = login()

        for slide_file in tqdm(slide_files, desc="Processing slides"):
            slide_date_time = get_slide_datetime(slide_file)

            if slide_date_time > enddatetime:
                print(f"Slide {slide_file} is after the cutoff date")
                continue

            accession_number = slide_file.split(";")[0]

            # Use test case ID from environment variables
            flag_info = get_retrieval_flag_info(token, accession_number)

            if flag_info:
                print(
                    f"Retrieval flag for case {accession_number}: {flag_info['retrieval_flag']}"
                )
            else:
                print(f"No retrieval flags found for case {accession_number}")

            retrieval_flag_df_dict["case_id"].append(accession_number)
            retrieval_flag_df_dict["slide_name"].append(slide_file)
            retrieval_flag_df_dict["slide_datetime"].append(slide_date_time)
            retrieval_flag_df_dict["retrieval_flag"].append(
                flag_info["retrieval_flag"] if flag_info else None
            )

        retrieval_flag_df = pd.DataFrame(retrieval_flag_df_dict)
        retrieval_flag_df.to_csv("retrieval_flags.csv", index=False)

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
