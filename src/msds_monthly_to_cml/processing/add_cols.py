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
    