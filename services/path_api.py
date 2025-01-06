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
    """Logs in to the Path API and retrieves an access token."""
    login_url = f"{PATH_API_HOST}/login"
    login_data = {"username": USERNAME, "password": PASSWORD}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # Remove verify=False and either:
    # A) Let it use the system's CA certificates:
    response = requests.post(login_url, data=urlencode(login_data), headers=headers)
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
    try:

        from tqdm import tqdm

        slide_dir = "/pesgisipth/NDPI"

        # get a list of filenames in slide_dir that start with "H22" and end with ".ndpi"
        slide_files = [
            f
            for f in os.listdir(slide_dir)
            if f.startswith("H22") and f.endswith(".ndpi")
        ]

        # Get authentication token
        token = login()

        for slide_file in tqdm(slide_files, desc="Processing slides"):
            accession_number = slide_file.split(";")[0]

            # Use test case ID from environment variables
            flag_info = get_retrieval_flag_info(token, accession_number)

            if flag_info:
                print(
                    f"Retrieval flag for case {accession_number}: {flag_info['retrieval_flag']}"
                )
            else:
                print(f"No retrieval flags found for case {accession_number}")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
