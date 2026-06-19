import pandas as pd


def get_reporting_period_string(df, column_name='reporting_period_start_datetime'):
    """
    Grabs the first datetime value from the column and returns it as a 'YYYY-MM' string.
    """
    if df.empty or column_name not in df.columns:
        return None
    first_value = df[column_name].iloc[0]
    timestamp = pd.to_datetime(first_value)

    return timestamp.strftime('%Y-%m')


def get_unique_values_in_group(
    df_input, 
    group_col="metric_id",
    value_col="location_type",
    new_col_name="available_locations",
    output_file="data_out/available_locations.csv"
):
    df_input
    df_grouped = (df_input
        .groupby(group_col)[value_col]
        .agg(lambda x: list(set(x)))
        .reset_index(name=new_col_name)
    )
    df_grouped.to_csv(output_file, index=False)

    return df_grouped
