# -------------------------------------------------
# Purpose: Import OSI Data as metadata and registers Dataframe as DataRobot Dataset
# * Note,this does not generate timeseries rows. Unable to query the PI Data Archive
#
# Steps:
# XML files are stored in GitHub
# Upload XML into DataRobot Codespace from GitHub
# Convert Raw XML into DataRobot Dataset
#    - Load the raw PI AF XML
#    - Parse the AF Element + Attribute hierarchy
#    - Produce a flattened AF metadata pandas DataFrame
#    - Register that DataFrame as a DataRobot Dataset
# -------------------------------------------------

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
XML_RAW_URL = "https://raw.githubusercontent.com/lawesworks/osidata/refs/heads/main/well-drilling-2015/OSIdemo_BASIC_UOG_Well_Drilling_Completion_Monitoring.xml"
DATAROBOT_DATASET_NAME = "Well Drilling & Completion"

XML_RAW_URL = "https://raw.githubusercontent.com/lawesworks/osidata/refs/heads/main/windfarm-2017/UOM_Price_and_EnergyPrice_v2017.xml"
DATAROBOT_DATASET_NAME = "Wind Farm Energy Price"

# -------------------------------------------------
# Download File from GitHub
# -------------------------------------------------

import requests

url = XML_RAW_URL

resp = requests.get(url)
resp.raise_for_status()

xml_content = resp.text

# Save locally so your existing parser works unchanged
with open("af_model.xml", "w", encoding="utf-8") as f:
    f.write(xml_content)

XML_PATH = "af_model.xml"

print("XML File Download Attempted")

# -------------------------------------------------
# Check if File Exists
# -------------------------------------------------
import os

# Verify
assert os.path.exists(XML_PATH), "XML file missing after download"
print("XML downloaded successfully\n")

#if os.path.exists(XML_PATH):
#    print(f"File exists: {XML_PATH}")
#else:
#    print(f"File NOT found: {XML_PATH}")

# -------------------------------------------------
# Sanity Check - Print out first 10 lines / confirm
# -------------------------------------------------

# Print the first few lines for preview
def print_file_head(path, n_lines=10):
    with open(path, "r", encoding="utf-8") as f:
        for i in range(n_lines):
            line = f.readline()
            if not line:
                break
            print(line.rstrip())

print("Preview:")
print_file_head(XML_PATH, n_lines=1)

# -------------------------------------------------
# import the required libraries
# -------------------------------------------------


import os
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

import pandas as pd

print("Imported Required Libraries")


# -------------------------------------------------
# XML HELPERS
# -------------------------------------------------

def _txt(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    t = elem.text.strip()
    return t if t else None


def _walk_attributes(
    attr_elem: ET.Element,
    element_path: str,
    element_name: str,
    element_template: Optional[str],
    attr_path_prefix: str,
    rows: List[Dict],
):
    """
    Recursively walk AFAttribute nodes and flatten them.
    """
    attr_name = _txt(attr_elem.find("Name")) or "(unnamed)"
    attr_path = f"{attr_path_prefix}/{attr_name}" if attr_path_prefix else attr_name

    rows.append({
        "element_path": element_path,
        "element_name": element_name,
        "element_template": element_template,
        "attribute_path": attr_path,
        "attribute_name": attr_name,
        "attribute_type": _txt(attr_elem.find("Type")),
        "data_reference": _txt(attr_elem.find("DataReference")),
        "config_string": _txt(attr_elem.find("ConfigString")),
        "static_value": _txt(attr_elem.find("Value")),
        "description": _txt(attr_elem.find("Description")),
    })

    # Nested attributes
    for child in attr_elem.findall("AFAttribute"):
        _walk_attributes(
            child,
            element_path,
            element_name,
            element_template,
            attr_path,
            rows,
        )


def _walk_elements(element: ET.Element, parent_path: str, rows: List[Dict]):
    """
    Recursively walk AFElement hierarchy.
    """
    element_name = _txt(element.find("Name")) or "(unnamed element)"
    element_template = _txt(element.find("Template"))
    element_path = f"{parent_path}/{element_name}" if parent_path else element_name

    for attr in element.findall("AFAttribute"):
        _walk_attributes(attr, element_path, element_name, element_template, "", rows)

    for child in element.findall("AFElement"):
        _walk_elements(child, element_path, rows)


def parse_af_xml(xml_path: str) -> pd.DataFrame:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    db = root.find("./AFDatabase")
    if db is None:
        raise ValueError("AFDatabase node not found — is this a PI AF export?")

    rows: List[Dict] = []
    for top_element in db.findall("./AFElement"):
        _walk_elements(top_element, "", rows)

    df = pd.DataFrame(rows)

    # Column order optimized for DataRobot + demos
    ordered_cols = [
        "element_path",
        "element_name",
        "element_template",
        "attribute_path",
        "attribute_name",
        "attribute_type",
        "data_reference",
        "config_string",
        "static_value",
        "description",
    ]
    df = df[[c for c in ordered_cols if c in df.columns]]

    return df

print("Built XML Helper Functions")

# -------------------------------------------------
# DATAROBOT DATASET REGISTRATION
# -------------------------------------------------
def register_in_datarobot(df: pd.DataFrame, dataset_name: str):
    import datarobot as dr

    # Uses ~/.config/datarobot/drconfig.yaml
    # or DATAROBOT_ENDPOINT / DATAROBOT_API_TOKEN env vars
    dr.Client()

    #dataset = dr.Dataset.upload(df)
    dataset = dr.Dataset.upload(df)
    try:
        dataset.modify(name=dataset_name)
        print(f"\n\nSuccess: dataset renamed to: {dataset.name}\n\n")
    except Exception as e:
        print(f"Warning: dataset uploaded but could not be renamed: {e}")
    print(f"Dataset uploaded to DataRobot. ID: {dataset.id}")
    print(f"Suggested Dataset Name: {dataset_name}")

    return dataset

print("Built DataRobot Dataset Creation Function")

# -------------------------------------------------
# DATAROBOT DATASET CONFIRMATION
# 
# DataRobot processes uploads asynchronously. 
# The Dataset object you get back immediately from dr.Dataset.upload() 
# can have row_count set while column count remains None until the platform finishes
# indexing the dataset.  
# 
# For this reason, two more robust helpers are created below 
# -------------------------------------------------
def print_dataset_summary(dataset):
    """
    Print a concise summary of a DataRobot Dataset.
    """
    if dataset is None:
        raise ValueError("Dataset is None. Was the upload successful?")

    dataset_id = getattr(dataset, "id", None)
    rows = getattr(dataset, "row_count", None)
    cols = getattr(dataset, "column_count", None)

    print("📊 DataRobot Dataset Summary")
    print(f"  • Dataset ID: {dataset_id}")
    print(f"  • Rows: {rows}, Columns: {cols}")


print("Built DataRobot Dataset Confirmation Function")

# -------------------------------------------------
# DATAROBOT VALIDATE ROWS / COLUMNS
# -------------------------------------------------

def quick_local_and_platform_check(df, dataset):
    """
    Print local DataFrame shape (definitive) and what the DataRobot dataset object reports.
    Use this immediately after upload to confirm things.
    """
    # Local truth
    print("Local pandas DataFrame:")
    print(f"  • Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(f"  • Column names: {list(df.columns)}\n")

    print(".\n\n")
    # Platform-reported (may be None if still processing)
    dataset_id = getattr(dataset, "id", None)
    rows = getattr(dataset, "row_count", None)
    cols = getattr(dataset, "column_count", None)

    print("DataRobot Dataset object (immediate):")
    print(f"  • Dataset ID: {dataset_id}")
    print(f"  • Rows: {rows}, Columns: {cols}")

import time
import datarobot as dr

def get_dataset_summary_with_refresh(dataset, timeout=60, interval=3):
    """
    Given a DataRobot Dataset object (returned from dr.Dataset.upload),
    try to retrieve its row/column counts. If column_count is None,
    poll the platform by retrieving the dataset again until populated or timeout.

    - dataset: the dataset object returned by dr.Dataset.upload(df)
    - timeout: total seconds to wait before giving up
    - interval: seconds between polls

    Returns a dict with id, rows, columns (columns may still be None if timed out).
    """
    if dataset is None:
        raise ValueError("dataset is None; upload may have failed.")

    dataset_id = getattr(dataset, "id", None)

    # Try a fast path first
    rows = getattr(dataset, "row_count", None)
    cols = getattr(dataset, "column_count", None)

    start = time.time()
    while cols is None and (time.time() - start) < timeout:
        try:
            # Try to fetch fresh dataset metadata from the platform
            # dr.Dataset.get is a safe, commonly-available SDK call; if not available in your SDK,
            # this will raise and we'll break out gracefully.
            refreshed = dr.Dataset.get(dataset_id)
            rows = getattr(refreshed, "row_count", rows)
            cols = getattr(refreshed, "column_count", cols)
            # If now populated, replace dataset object for caller convenience
            dataset = refreshed
        except Exception as e:
            # If the SDK call fails, print debug info and continue trying until timeout
            print(f"[debug] refresh attempt failed: {e}")

        if cols is None:
            time.sleep(interval)

    # Print summary
    print("📊 DataRobot Dataset Summary (refreshed)")
    print(f"  • Dataset ID: {dataset_id}")
    print(f"  • Rows: {rows}, Columns: {cols}")
    return {"dataset": dataset, "id": dataset_id, "rows": rows, "columns": cols}

# -------------------------------------------------
# MAIN
# -------------------------------------------------

print("Executing End to End Main Function")

def main():
    if not os.path.exists(XML_PATH):
        raise FileNotFoundError(f"XML not found at {XML_PATH}")

    # 1–3) XML → AF Metadata DataFrame
    df = parse_af_xml(XML_PATH)

    print(f"Parsed {len(df)} AF attributes")
    print(df.head(10).to_string(index=False))

    # 4) Register dataset in DataRobot
    dataset = register_in_datarobot(df, DATAROBOT_DATASET_NAME)

    # 5) Confirm Dataset is loaded into DataRobot
    
    print(".\n\n")
    print_dataset_summary(dataset)

    print(".\n\n")
    # quick local check
    quick_local_and_platform_check(df, dataset)

    print(".\n\n")
    # then refresh/poll to get accurate column_count (waits up to 60s by default)
    summary = get_dataset_summary_with_refresh(dataset, timeout=60, interval=2)
    



if __name__ == "__main__":
    main()
