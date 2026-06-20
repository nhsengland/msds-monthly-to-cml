import os

import pandas as pd
import numpy as np


def add_location_type_id_col(
    df_maternity: pd.DataFrame, df_reference: pd.DataFrame
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
        df_result["org_type_description"],
    ]
    df_result["location_type"] = np.select(conditions, outputs, default=None)

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
