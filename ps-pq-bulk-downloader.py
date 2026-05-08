#!/usr/bin/env python3.13
# -*- coding: utf-8 -*-

"""
Module Document String

This script demonstrates how to pull data from a PowerSchool plugin by 
gathering the bearer token from PowerSchool, then using that bearer token 
to authenticate with your PowerSchool API and save the results to an Excel
file using Pandas. 

It includes error handling for most of the errors you might come across 
when interacting with the PowerSchool API, loading the API client 
credentials from an .env file (or environment variables) for security.

The API endpoints are enumerated in a separate config.ini file as a 
central configuration point for all of your exports, and the script
supports multiple sections in the config.ini file if you want to set up
multiple exports with different query endpoints, output filenames, and 
other options.

"""

__author__ = "Gregory Matyola"
__version__ = "2026.05.07"
__license__ = "MIT"

import configparser  # For reading configuration from .env file, including support for multiple sections for different exports
import json  # For handling JSON data in API requests and responses
from os import getenv  # For accessing environment variables securely without hardcoding sensitive information
from pathlib import Path  # For handling file paths in a cross-platform way
import sys  # For exiting the script with specific status codes on errors
import traceback
from typing import NoReturn  # For exiting the script with specific status codes on errors
try:
    import requests  # For making HTTP requests to the API
except ImportError:
    print("The 'requests' library is required to run this script. Please install it using 'pip install requests' and try again.")
    sys.exit(1)
try:
    import pandas as pd  # For data manipulation and saving to Excel
except ImportError:
    print("The 'pandas' library is required to run this script. Please install it using 'pip install pandas' and try again.")
    sys.exit(1)
try:
    from dotenv import load_dotenv  # For loading environment variables from a .env file
except ImportError:
    print("The 'python-dotenv' library is required to run this script. Please install it using 'pip install python-dotenv' and try again.")
    sys.exit(1)
try:
    from requests.auth import (
        HTTPBasicAuth,
    )  # For handling HTTP Basic Authentication when obtaining the Bearer token
except ImportError:
    print("The 'requests' library is required to run this script. Please install it using 'pip install requests' and try again.")
    sys.exit(1)
from inspect import cleandoc  # For cleaner multi-line docstrings and error messages
try:
    from loguru import logger # For better logging and debugging
except ImportError:
    print("The 'loguru' library is required to run this script. Please install it using 'pip install loguru' and try again.")
    sys.exit(1)

EMPTY_DATAFRAME = pd.DataFrame()  # Define an empty DataFrame to return in case of errors or no data

SAMPLE_ENV: str = cleandoc(doc="""
# Sample .env file loaded by the script via the load_dotenv() function above.
# This file should be in the same directory as your script and 
# *** should not be committed to version control ***
# *** because it contains sensitive information. ***

# CLIENT_ID and CLIENT_SECRET from your plugin registration in PowerSchool. 
# These should be kept secret and NEVER hardcoded in a script.
                           
CLIENT_ID=
CLIENT_SECRET=
                           
""") # SAMPLE_ENV

SAMPLE_CONFIG_INI: str = cleandoc(doc="""
# Auth_URL should be your https://[YOUR_POWERSCHOOL_URL]/oauth/access_token
AUTH_URL=https://[YOUR_POWERSCHOOL_URL]/oauth/access_token

# If you're only pulling one from your powerquery, you can use the default 
# '<UNNAMED_SECTION>' config file section:
                                  
# Data_URL (in the "blank" config should be https://[YOUR_POWERSCHOOL_URL]/ws/schema/query/[YOUR_PS_QUERY_NAME]/?pagesize=0
DATA_URL=https://[YOUR_POWERSCHOOL_URL]/ws/schema/query/[YOUR_PS_QUERY_NAME]/?pagesize=0

# OUTPUT_FILE is the default filename to output to...
OUTPUT_FILE=powerquery_export.xlsx

# ARGUMENTS is an optional comma-separated list of arguments to pass in the API request body, if your query requires any. 
# For example, if your query has 'terms_start=21' and 'terms_end=36' as arguments, you would set the following:
# ARGUMENTS={
#           "terms_start":"21",
#           "terms_end":"36"
#           }
ARGUMENTS={
          "terms_start":"21",
          "terms_end":"36"
          }

# Each export you want to run is set up in a [SECTION] 
# Section "CURRICULUM_EXPORT" is an example of how to set up one export, but you can have as many sections as you want for different exports,
# and they can all have different DATA_URLs, OUTPUT_FILEs, and REQUIRED_COLUMNS if needed. Just make sure to follow the same format for each section.

[CURRICULUM_EXPORT]
# Summary is an optional boolean flag to include a summary of the data in the logs, including total records, columns, sample data, and group by counts for the first two columns. This can be helpful for verifying that the data looks correct before saving to Excel.
SUMMARY=true
                           
# SKIP is an optional boolean flag to skip this section if you want to temporarily disable an export without removing its configuration.
SKIP=false                           

# OUTPUT_FILE is filename to output the data to the category section...
OUTPUT_FILE=curriculum_export.xlsx

# Data_URL tells the script where to pull data from for that export, and can be different for each section if you have multiple exports. 
# It should be in the format of https://[YOUR_POWERSCHOOL_URL]/ws/schema/query/[YOUR_PS_QUERY_NAME]/?pagesize=0
DATA_URL=https://[YOUR_POWERSCHOOL_URL]/ws/schema/query/[YOUR_PS_QUERY_NAME]/?pagesize=0

# REQUIRED_COLUMNS is what colunms you require for that export, and in what order. 
# This is important to set up correctly in order for the script to know how to parse the API response and save it to Excel in the correct order.
REQUIRED_COLUMNS=SCHOOL_YEAR, SCHOOL_CODE, COURSE_CODE, COURSE_DESCRIPTION, DEPARTMENT_CODE, MAXIMUM_SEATS, COURSE_CREDITS, ATTENDANCE_TAKEN, GRADED_COURSE, INCLUDE_IN_HONOR_ROLL, INCLUDE_IN_GPA, GPA_WEIGHT_CODE, GPA_WEIGHT, INCLUDE_ON_TRANSCRIPT, ADVANCED_PLACEMENT_COURSE, COLLEGE_PREP_COURSE, HONORS_COURSE, ADVANCED_LEVEL, ACCELERATED, ELEMENTARY_COURSE, STUDY_HALL_FLAG, LUNCH_FLAG, SPECIAL_ED_COURSE, PRE-REQUISITES, TECH_PREP, NCAA_CORE_COURSE, PRIMARY_SUBJECT_AREA_CODE, SCED_COURSE_CODE, SCED_COURSE_LEVEL, SCED_SEQUENCE, SCED_GRADE_SPAN

# ACCEPTABLE_BLANK_COLUMNS Columns that you want to include in your output, even if they're not included in the API response.
# This is useful for columns where every row is either null or blank, but you need to have the columns present in your Excel output with blank values. Just make sure to include them in your plugin's named_queries.xml as well, and give them field level access in your plugin's access_request.
# ACCEPTABLE_BLANK_COLUMNS=DEPARTMENT_CODE, COURSE_CREDITS, GPA_WEIGHT_CODE, PRIMARY_SUBJECT_AREA_CODE
                           
# ARUGUMENTS is an optional comma-separated list of arguments to pass in the API request body, if your query requires any. 
# For example, if your query has 'terms_start=21' and 'terms_end=36' as arguments, you would set the following:
ARGUMENTS={
          "terms_start":"21",
          "terms_end":"36"
         }
""")  # SAMPLE_CONFIG_INI


def fetch_bearer_token(client_id: str, client_secret: str, auth_url: str) -> str | None:
    """
    Authenticates with the PowerSchool API using the OAuth2 Client Credentials
    grant type to obtain a short-lived Bearer token.

    Args:
        client_id: The plugin client ID from PowerSchool's Plugin Configuration.
        client_secret: The corresponding client secret for the plugin.
        auth_url: The full OAuth token endpoint URL, e.g.
            https://[YOUR_PS_URL]/oauth/access_token

    Returns:
        str | None: The access token string if authentication succeeded,
            or None if the response did not include one.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails
            or the server returns a 4xx/5xx status code.
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        # Some APIs require scope, e.g., 'scope': 'read_data'
    }

    try:
        # Note: Many OAuth providers (like Auth0, Okta) accept Basic Auth for the client_id/secret.
        # If your API requires them in the body, add them to the 'data' dictionary instead.
        response = requests.post(
            auth_url,
            data=data,
            headers=headers,
            auth=HTTPBasicAuth(client_id, client_secret),
            timeout=10,
        )

        # Check for HTTP errors (4xx or 5xx)
        response.raise_for_status()

        token_data = response.json()
        return token_data.get("access_token")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error obtaining token: {e}")
        if e.response is not None:
            logger.error(
                f"Remote Server response status code: {e.response.status_code}"
            )
            logger.error(f"Remote Server response body: {e.response.text}")
            if "error" in e.response.text.lower():
                logger.critical(cleandoc(doc="""
                This error often indicates an issue with your client credentials or the authentication endpoint.
                Please check the following:
                ** 1. Verify that your CLIENT_ID and CLIENT_SECRET are correct and match what is registered in PowerSchool.
                ** 2. Ensure that your plugin has been enabled in the PowerSchool Plugin Configuration page.
                ** 3. Ensure that your AUTH_URL is correct and points to the /oauth/access_token endpoint of your PowerSchool instance.
                ** 4. Check if there are any additional requirements for authentication, such as specific scopes or headers.
                """))
        raise e # Can't do anything more here. Raise an exception to be caught in main().


def construct_count_url(data_url: str) -> str:
    """
    Derives the /count endpoint URL from a PowerSchool data query URL by
    stripping query parameters and appending '/count'.

    Args:
        data_url: The full data query URL, e.g.
            https://[PS_URL]/ws/schema/query/[QUERY_NAME]/?pagesize=0

    Returns:
        str: The constructed count endpoint URL, or an empty string if
            data_url is falsy.
    """
    if not data_url:
        return ''
    base_url = data_url.split("?")[0].rstrip("/")
    return f"{base_url}/count"

def fetch_data_count(count_url: str, headers: dict, body: dict) -> int | None:
    """
    Queries the /count endpoint to retrieve the expected number of records
    before fetching the full dataset, used to verify completeness after
    the main fetch.

    Args:
        count_url: The /count endpoint URL, typically built by construct_count_url().
        headers: HTTP headers to include, such as the Authorization bearer token.
        body: JSON-serializable request payload. The __debug_query key is
            stripped before sending to avoid inflating the count.

    Returns:
        int | None: The expected record count, or None if the endpoint is
            unavailable or the request fails.
    """
    if not count_url:
        logger.warning("Count URL not available.")
        return None

    # Clean body for count request
    count_body = (body or {}).copy()
    count_body.pop("__debug_query", None)

    try:
        resp = requests.post(count_url, headers=headers, json=count_body, timeout=20)
        if resp.status_code == 200:
            count = resp.json().get("count")
            logger.trace(f"Total records to fetch: {count}")
            return count
        logger.warning(f"Count fetch failed ({resp.status_code}): {resp.text}")
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"Error fetching count: {e}. Proceeding anyway.")
    return None

def manage_api_error_responses(response, response_json) -> bool:
    """
    Inspects a PowerSchool API response for known error patterns and logs
    actionable troubleshooting steps for each case.

    Handles three cases: query-not-found, access validation failures, and
    generic HTTP errors. Does not raise on the first two — returns False
    so the caller can decide how to proceed. Unhandled HTTP errors are
    re-raised via response.raise_for_status().

    Args:
        response: The raw requests.Response object from the API call.
        response_json: The parsed JSON body of the response as a dict.

    Returns:
        bool: True if no error was detected, False if a handled error occurred.

    Raises:
        requests.exceptions.HTTPError: For unhandled non-200 HTTP responses,
            via response.raise_for_status().
    """
    message = response_json.get("message", "")
    status = response.status_code

    if status == 200 and "error" not in message.lower():
        return True  # No error, proceed as normal

    logger.error(f"Error: Received status code {status}")
    
    # Case: Query Not Found
    if "Query" in message and "not found" in message:
        logger.critical(cleandoc("""
            Query not found. Check:
            1. Name matches DATA_URL.
            2. Plugin is active in PowerSchool.
            3. defined in named_queries.xml.
            4. API user has permissions.
        """))
        return False

    # Case: Validation Failed (Access Request Fix)
    if message == "Validation Failed":
        fields = []
        for error in response_json.get("errors", []):
            parts = error.get("field", "").split(".")
            if len(parts) >= 2:
                fields.append(f'<field table="{parts[0]}" field="{parts[1]}" access="ViewOnly" />')
        
        quickfix = "\n".join(fields)
        logger.critical(f"Validation Failed. Add to <access_request>:\n{quickfix}\n</access_request>")
        return False

    # Default Error Handling
    logger.error(f"Response: {response.text}")
    response.raise_for_status()
    return False # Should not reach here due to raise_for_status(), but included for clarity

def process_response_and_handle_errors(data_url, headers=None, body=None) -> dict:
    """
    Sends a POST request to the data URL and returns the parsed API response. This function also 
    validates response counts and delegates detailed error handling to PowerSchool-specific helpers.

    The function compares expected and actual record counts, logs useful diagnostics, and returns an 
    empty dict when errors are handled gracefully.
    
    Args:
        data_url: The full URL of the PowerSchool data endpoint to query.
        headers: Optional HTTP headers to include in the request, such as authorization and content type.
        body: Optional JSON-serializable payload to send in the POST request.

    Returns:
        dict: The JSON decoded response body from the API, or an empty dict if a handled error occurs.
    Raises:
        requests.exceptions.RequestException: If the POST request fails due to a network error or connection timeout.
    """
    # 1. Fetch expected count
    count_url = construct_count_url(data_url)
    expected_count = fetch_data_count(count_url, headers, body)

    # 2. Fetch main data
    logger.trace(f"Requesting data from: {data_url}")
    try:
        response = requests.post(data_url, headers=headers, json=body, timeout=20)
        response_json = response.json() if response.content else {}
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        if e.response:
            logger.error(f"Remote Server ({e.response.status_code}): {e.response.text}")
        raise

    # 3. Handle specific PowerSchool errors
    if not manage_api_error_responses(response, response_json):
        return {}  # Return empty dict on handled errors to prevent further processing

    # 4. Success Logging & Verification
    records = response_json.get("record", [])
    actual_count = len(records)
    
    logger.trace(f"Data fetched successfully ({response.status_code}).")
    
    if expected_count is not None and actual_count != expected_count:
        logger.warning(f"Mismatch! Received {actual_count} records, expected {expected_count}.")

    if records:
        logger.trace(f"Received {actual_count} records. Sample: {str(records[0])[:100]}...")
    else:
        logger.warning("No records received from API.")

    return response_json

def fetch_api_data(queryOptions=None, token=None) -> dict:
    """
    Builds the authorization headers and request body from queryOptions,
    then delegates to process_response_and_handle_errors() to fetch data
    from the PowerSchool API.

    Args:
        queryOptions: A dict of export configuration including DATA_URL,
            ARGUMENTS, and DEBUG flag. Returns None immediately if absent.
        token: The Bearer token string from fetch_bearer_token(). Returns
            None immediately if absent.

    Returns:
        dict: The parsed API response, or an empty dict on handled
            errors, or if queryOptions or token are missing.

    Raises:
        SystemExit: If a requests.RequestException occurs during the fetch.
    """
    if not queryOptions or not token:
        logger.error("Error: Missing query options or token.")
        return {}
        
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    logger.trace(f"   Headers prepared for API request: {headers}")

    if queryOptions and queryOptions.get("ARGUMENTS"):
        body = json.loads(queryOptions.get("ARGUMENTS", "{}"))
    else:
        body = {}

    if queryOptions.get("DEBUG"):
        body["__debug_query"] = "true" # Add debug flag to the body to get additional information in the API response for troubleshooting
        logger.warning(f"   Body using __debug_query, performance may be affected!: {body}")

    if body:
        logger.trace(f"   Body prepared for API request: {body}")
    else:
        logger.warning("   No body to include in API request.")

    logger.trace(f"   Arguments provided for API request: {body}")

    try:
        return process_response_and_handle_errors(
            data_url=queryOptions.get("DATA_URL"), headers=headers, body=body
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        sys.exit(1)


def normalize_data(data, queryOptions=None) -> pd.DataFrame:
    """
    Converts raw API records into a normalized, ordered pandas DataFrame
    ready for Excel export.

    Accepts a list of record dicts or a wrapper dict containing records
    under common keys ('data', 'results'). Renames all columns to
    snake_case, reorders them per REQUIRED_COLUMNS, and inserts blank
    columns for any ACCEPTABLE_BLANK_COLUMNS absent from the API response.

    Args:
        data: Raw records from the API — either a list of record dicts, or
            a dict wrapping them under 'data' or 'results' keys.
        queryOptions: Export configuration dict containing REQUIRED_COLUMNS,
            ACCEPTABLE_BLANK_COLUMNS, and SUMMARY flag.

    Returns:
        pd.DataFrame: The normalized and reordered DataFrame, or an empty
            DataFrame if no records were found or an error occurred.
    """

    # NORMALIZATION:
    # APIs often return data wrapped in keys like 'data', 'results', or 'items'.
    # You may need to inspect your API response and adjust the logic below.

    records = []
    try:
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Try to find common list keys, or default to the whole dict
            if "data" in data and isinstance(data["data"], list):
                records = data["data"]
            elif "results" in data and isinstance(data["results"], list):
                records = data["results"]
            else:
                records = [data]  # Wrap single object in list

        if not records:
            logger.warning("No records found to save.")
            return EMPTY_DATAFRAME

        # Create DataFrame
        df = pd.DataFrame(data=records, dtype=str)
        original_columns = df.columns.tolist()  # Store original columns for debugging
        logger.trace(f"Original columns: {original_columns}")
        df = df.rename(columns=lambda col: col.strip().lower().replace(" ", "_"))
        logger.trace(f"Columns after normalization: {df.columns}")
        # Desired column order for some columns
        columns_desired_order = [
            col.strip().lower().replace(" ", "_")
            for col in (
                queryOptions.get("REQUIRED_COLUMNS").split(sep=",") if queryOptions else []
            )
        ]

        # Ensure no columns are dropped by adding the rest automatically
        remaining_cols = [col for col in df.columns if col not in columns_desired_order]
        new_order = columns_desired_order + remaining_cols
        logger.trace(f"New column order: {new_order}")

        for col in (
            queryOptions.get("ACCEPTABLE_BLANK_COLUMNS", "").split(sep=",")
            if queryOptions and queryOptions.get("ACCEPTABLE_BLANK_COLUMNS")
            else []
        ):
            fix = col.strip().lower().replace(" ", "_")
            logger.trace(
                f"Ensuring column '{fix}' is included in the DataFrame, even if it's blank."
            )
            if fix not in df.columns:
                df[fix] = (
                    ""  # Add the column with blank values if it's not already in the DataFrame
                )
        # Reorder DataFrame
        try:
            df = df[new_order]
        except KeyError as e:
            logger.error(
                f"KeyError: {e} \n**This often indicates that an expected key is missing from the API response. \n** Please check the structure of the API response and ensure it matches what the script expects."
            )
            (
                logger.warning(original_columns)
                if "original_columns" in locals()
                else logger.warning("Original columns not available for debugging.")
            )
            logger.error(traceback.format_exc())  # Print the raw data for debugging
            return EMPTY_DATAFRAME
        if queryOptions and queryOptions.get("SUMMARY", False):
            summarize_dataframe(queryOptions, df)
        logger.trace(df.head())
        logger.trace(df.columns)
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        logger.error(traceback.format_exc())  # Print the raw data for debugging
        return EMPTY_DATAFRAME
    return df


def summarize_dataframe(queryOptions, df) -> None:
    """
    Logs a concise summary of the exported DataFrame for quick validation. This helps confirm that the shape, columns, and sample values of the data match expectations before saving.

    The function reports total row count, column names, a sample of the first few rows, and basic value distributions for the first one or two columns.

    Args:
        queryOptions: A configuration mapping that may control whether summaries are emitted or how they are interpreted.
        df: The pandas DataFrame to summarize in the logs.

    Returns:
        None: This function logs information but does not return a value.
    """
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Sample data:\n{df.head().to_string(index=False)}")
    if len(df.columns) > 0:
        logger.info(f"Group by ({df.columns[0]}):\n{df[df.columns[0]].value_counts()}")
    if len(df.columns) > 1:
        logger.info(f"Group by ({df.columns[1]}):\n{df[df.columns[1]].value_counts()}")


def save_to_excel(dataFrame: pd.DataFrame, queryOptions=None):
    """
Writes a normalized DataFrame to an Excel (.xlsx) file at the path
specified in queryOptions['OUTPUT_FILE'].

Args:
    dataFrame: The pandas DataFrame to write. If None or empty, logs
        a warning and returns without writing.
    queryOptions: A dict containing at minimum OUTPUT_FILE. If None,
        logs an error and returns early.
"""
    if queryOptions is None:
        logger.error("Error: Missing query options for saving to Excel.")
        return

    if dataFrame is None or dataFrame.empty:
        logger.warning("No data to save.")
        return

    try:
        dataFrame.to_excel(
            excel_writer=queryOptions.get("OUTPUT_FILE", "api_data.xlsx"), index=False
        )
        logger.info(
            f"\n+   Success! Data saved to {queryOptions.get('OUTPUT_FILE', 'api_data.xlsx')}"
        )

    except Exception as e:
        logger.error(f"    Error saving to Excel: {e}")


def process_categories(queryOptions=None, token=None, number=None, cat=None) -> pd.DataFrame:
    """
    Orchestrates fetching, inspecting, and normalizing API data for a single export category. This function coordinates logging, error handling, and conversion of raw API records into a DataFrame.

    The function retrieves records from the API, logs diagnostic information about the response, and returns a normalized DataFrame or an empty DataFrame when no usable data is available.

    Args:
        queryOptions: Configuration for the API request and normalization, including URLs, column settings, and flags.
        token: Bearer token used to authenticate the API request.
        number: Optional index of the category when processing multiple sections, used for log messages.
        cat: Optional name of the category being processed, used for log messages.

    Returns:
        pd.DataFrame: A normalized DataFrame containing the API records, or EMPTY_DATAFRAME if no data is available or an error occurs.
    """
    if queryOptions is None:
        return EMPTY_DATAFRAME
    
    api_data = fetch_api_data(queryOptions, token=token)

    if not api_data:
        logger.error("No data returned from API.")
        return EMPTY_DATAFRAME
    
    for key, value in api_data.items():
        if key == "record":
            my_records = value
            if my_records:
                logger.trace(f"   Number of records received: {len(my_records)}")
            else:
                logger.warning("No records received from API.")
            for _ in my_records:
                if _:
                    logger.trace(f"   Number of fields received: {len(_)}")
                else:
                    logger.warning("No fields received from API.")
                logger.trace(
                    f"{str(_)[:100]}..."
                )  # Print first 100 chars of each sub-key's value for verification
        logger.trace(
            f"   {key}: {str(value)}..."
        )  # Print first 100 chars of each key's value for verification
    logger.trace(
        f"   Sample data: {str(api_data)[:200]}..."
    )  # Print first 200 chars of the response for verification

    if not number:
        logger.info("\n3. Data received.")
    else:
        logger.info(f"\n3.{number} Data received for category {cat}.")
    try:
        my_records = api_data["record"]
        return normalize_data(my_records, queryOptions=queryOptions)
    except KeyError:
        logger.error(
            "Error: 'record' key not found in API response. Cannot save to Excel."
        )
        logger.error(
            f"Full API response: {api_data}"
        )  # Print full response for debugging
        return EMPTY_DATAFRAME
    except Exception as e:
        logger.error(f"Error processing API data: {e}")
        logger.error(
            f"Full API response: {api_data}"
        )  # Print full response for debugging
        return EMPTY_DATAFRAME

def abort_with_sample(message, sample_file) -> NoReturn:
    """
    Logs a critical error and exits the script with a non-zero status.
    This helper also prints a sample configuration or environment file
    to guide the user in fixing the issue.

    The function is intended for unrecoverable configuration or 
    authentication problems where the script cannot proceed safely.

    Args:
        message: A human-readable description of why the script is aborting.
        sample_file: A sample config or environment file content to display as guidance.

    Returns:
        NoReturn: This function terminates the program and never returns.
    """
    logger.error(message)
    logger.error(sample_file) # Print sample file content for user reference
    sys.exit(1)

def main() -> None:
    """
    Entry point for the PowerSchool export script.

    Loads credentials from .env, reads export configuration from config.ini,
    authenticates with the PowerSchool OAuth endpoint, then iterates over
    each configured export section to fetch, normalize, and save data to
    Excel. Falls back to the unnamed section if no named sections exist.
    """
    logger.info("\nStarting the export script...")
    # 1. Load Environment Variables
    # This loads the variables from the .env file into the script securely.
    load_dotenv()

    # Retrieve variables from .env via the .dotfiles package. These should be set in your .env file and will be used for authentication and API requests.
    CLIENT_ID = getenv("CLIENT_ID")
    CLIENT_SECRET = getenv("CLIENT_SECRET")
    # Validation to ensure env vars exist
    if not all((CLIENT_ID, CLIENT_SECRET)):
        abort_with_sample(
            "Error: Missing environment variables. Please check your .env file. See sample below!",
            SAMPLE_ENV,  # Print sample .env content for user reference if env vars are missing
        )
    config = configparser.ConfigParser(allow_unnamed_section=True)
    config.read(filenames="config.ini")  # Your config.ini file should be in the same directory as your script. 
    # This file is used to manage all configuration for the script, including API endpoints, output filenames, and other options. 
    # It also supports multiple sections for different exports if needed.
    default: configparser.SectionProxy = config[configparser.UNNAMED_SECTION]
    AUTH_URL: str | None             = default.get(option="AUTH_URL", fallback=None)
    DATA_URL: str | None             = default.get(option="DATA_URL", fallback=None)
    OUTPUT_FILE: str          = default.get(option="OUTPUT_FILE", fallback="api_data.xlsx")
    REQUIRED_COLUMNS: str     = default.get(option="REQUIRED_COLUMNS", fallback="")
    ACCEPTABLE_BLANK_COLUMNS: str = default.get(option="ACCEPTABLE_BLANK_COLUMNS", fallback="")
    ARGUMENTS: str            = default.get(option="ARGUMENTS", fallback="{}")
    SUMMARY: bool             = default.getboolean("SUMMARY", fallback=False)
    DEBUG: bool = default.getboolean("DEBUG", fallback=False)
    logger.info("\n   Configuration loaded.")
    logger.info(
        f"\n   Sections in config.ini file: {config.sections()}"
    )  # Print available sections for verification   
    if not AUTH_URL:
        abort_with_sample(
            "Error: Missing environment variables. Please check your config.ini file. See sample below!",
            SAMPLE_CONFIG_INI, # Print sample config.ini content for user reference if env vars are missing
        )
    logger.info("\n1. Authenticating...")
    try:
        token = fetch_bearer_token(CLIENT_ID, CLIENT_SECRET, AUTH_URL)
        logger.info("\n   Token received.")
    except Exception as e:
        logger.error(f"\n   Error during authentication: {e}")
        sys.exit(-1)

    logger.info("\n2. Fetching data...")

    # Iterating over the 'categories' section
    categories = config.sections()
    len_categories = len(categories) - 1 # Subtract 1 to exclude the unnamed default section, 
                                                        # which is used for global defaults but not used as an actual export category
    if len_categories > 0:        # Check if there are more than one sections (assuming *configparser.UNNAMED_SECTION* is one of them)
        # because we loaded the config with allow_unnamed_section=True, we can have both a default unnamed section for global defaults, 
        # and multiple named sections for different exports.
        for number, cat in enumerate(categories, start=0):
            logger.info(
                f"\n2.{number}/{len_categories} Fetching data for {cat}..."
            )
            if cat in (configparser.UNNAMED_SECTION,):
                logger.info(f"\n   Skipping {cat} ...")
                continue  # Skip the default section if it exists
            if not isinstance(cat, str):
                logger.warning(
                    f"\n   Warning: Category '{type(cat)=}' is not a string. Skipping."
                )
                continue  # Skip if cat is not a string (just a safety check)
            if config[cat].getboolean(option="SKIP", fallback=False) is True:
                logger.info(f"\n-     Skipping {cat} as per configuration...")
                continue  # Skip this category if SKIP is set to true in the config 
            queryOptions = {
                "DATA_URL": config[cat].get(
                    option="DATA_URL", fallback=DATA_URL # Use global DATA_URL as fallback if not set in category section
                ),
                "OUTPUT_FILE": config[cat].get(
                    option="OUTPUT_FILE",
                    fallback=f"{Path(OUTPUT_FILE).stem}_{cat}{Path(OUTPUT_FILE).suffix}", # Use Global .env OUTPUT_FILE as base, but add category name to the filename for multiple exports, e.g. "api_data.xlsx" becomes "api_data_CURRICULUM_EXPORT.xlsx
                ),
                "REQUIRED_COLUMNS": config[cat].get(
                    option="REQUIRED_COLUMNS", fallback=REQUIRED_COLUMNS # Use global REQUIRED_COLUMNS as fallback if not set in category section
                ),
                "ACCEPTABLE_BLANK_COLUMNS": config[cat].get(
                    option="ACCEPTABLE_BLANK_COLUMNS",
                    fallback=ACCEPTABLE_BLANK_COLUMNS, # Use global ACCEPTABLE_BLANK_COLUMNS as fallback if not set in category section
                ),
                "ARGUMENTS": config[cat].get(
                    option="ARGUMENTS", fallback=ARGUMENTS # Use global ARGUMENTS as fallback if not set in category section
                ),
                "SUMMARY": config[cat].getboolean(
                    option="SUMMARY", fallback=SUMMARY # Use global default of False for SUMMARY if not set in category section
                ),
                "DEBUG": config[cat].getboolean(
                    option="DEBUG", fallback=DEBUG # Use global DEBUG value as fallback if not set in category section
                ),
            }
            df = process_categories(
                queryOptions=queryOptions, token=token, number=number, cat=cat
            )
            logger.info(f"\n4.{number} Saving to Excel for category {cat}...")
            save_to_excel(dataFrame=df, queryOptions=queryOptions)

    else:
        queryOptions = {
            "DATA_URL": DATA_URL,
            "OUTPUT_FILE": OUTPUT_FILE,
            "REQUIRED_COLUMNS": REQUIRED_COLUMNS,
            "ACCEPTABLE_BLANK_COLUMNS": ACCEPTABLE_BLANK_COLUMNS,
            "ARGUMENTS": ARGUMENTS,
            "SUMMARY": SUMMARY,
            "DEBUG": DEBUG,
        }
        df = process_categories(queryOptions=queryOptions, token=token)
        logger.info("\n4. Saving to Excel...")
        save_to_excel(dataFrame=df, queryOptions=queryOptions)


if __name__ == "__main__":
    logger.info("\nStarting the export script...")
    main()
    logger.info("\nExport script finished! Job's Done!")
