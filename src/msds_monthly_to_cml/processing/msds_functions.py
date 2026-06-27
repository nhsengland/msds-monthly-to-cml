import os
import sys
import logging
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def add_location_type_id_col(
    df_maternity: pd.DataFrame, 
    df_reference: pd.DataFrame,
    non_place_codes,
) -> pd.DataFrame:
    """Left joins reference data on 'location_id', replaces 'location_type'
    with the formats used in all_places on FDP, fills in any gaps with 
    the reference data, and cleans up the lookup columns.
    """
    df_result = df_maternity.copy()
    
    df_result = df_result.merge(df_reference, on="location_id", how="left")
    
    conditions = [
        df_result["location_type"] == "National",
        df_result["location_type"] == "NHS England (Region)",
        df_result["location_type"] == "SubICB of Responsibility",
        df_result["location_type"] == "Local Authority of Residence",
        df_result["location_type"] == "Local Maternity System",
        df_result["location_type"] == "Provider",
        df_result["location_type"] == "MBRRACE Grouping",
        df_result["location_id"].isin(non_place_codes),
        df_result["org_type_description"].notna(),
    ]
    outputs = [
        "nhs-country",
        "nhs-region",
        "nhs-sub-icb-location",
        "local-authority",
        "nhs-icb",
        "nhs-trust",
        "england",
        "non-place-code",
        df_result["org_type_description"],
    ]
    df_result["location_type"] = np.select(conditions, outputs, default='invalid')

    df_result.drop(columns=["org_type_description"], inplace=True)

    return df_result
    

def filter_out_existing_dimensions(
    df_dimensions: pd.DataFrame,
    output_path: str
) -> pd.DataFrame:
    """
    Filters out dimensions from df_dimensions that already exist in CSV files
    with "dimension" in their filename in the output_path directory.

    Args:
        df_dimensions: DataFrame containing dimension data with a 'dimension_id' column
        output_path: Path to directory containing CSV files to check against

    Returns:
        Filtered DataFrame with only new dimensions
    """
    dimension_files = [
        f for f in os.listdir(output_path)
        if f.endswith('.csv') and 'dimension' in f.lower()
    ]

    existing_ids = set()

    for file in dimension_files:
        file_path = os.path.join(output_path, file)
        try:
            df = pd.read_csv(file_path)
            if 'dimension_id' in df.columns:
                existing_ids.update(df['dimension_id'].dropna().unique())
        except Exception as e:
            print(f"Warning: Could not read file {file}: {str(e)}")
            continue

    df_filtered = df_dimensions[~df_dimensions['dimension_id'].isin(existing_ids)]

    return df_filtered


def get_metric_status(config: dict) -> str:
    """
    Determines whether data is 'act' (actual/final) or 'prov' (provisional).

    Args:
        config: dict with a required 'status' key.

    Returns:
        'prov' if status is 'prov' or 'provisional' (case-insensitive).
        'act' if status is 'actual' or 'final' (case-insensitive).

    Raises:
        KeyError:   if 'status' is missing from config.
        ValueError: if 'status' is an unrecognised value.
    """
    if "status" not in config:
        raise KeyError("'status' is missing from config.")

    status = config["status"].strip().lower()

    if status in ("prov", "provisional"):
        return "prov"
    if status in ("actual", "final"):
        return "act"

    raise ValueError(
        f"Unrecognised status value: {config['status']!r}. "
        f"Expected one of: 'prov', 'provisional', 'actual', 'final'."
    )

def fix_location_id_unknowns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Where location_id is the string 'null' or 'Unknown',
    concatenate location_id and location_type with an underscore.
    All other rows are left unchanged.
    """
    mask = df["location_id"].isin(["null", "Null", "Unknown", "unknown"])
    df = df.copy()  # avoid mutating the original
    df.loc[mask, "location_id"] = (
        df.loc[mask, "location_id"] + "_" + df.loc[mask, "location_type"]
    )
    return df

