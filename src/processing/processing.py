from pyspark.sql import functions as F
from pyspark import sql as pyspark


def move_attributes_to_new_dimension(
    df, 
    existing_dimension_col_name,
    existing_dimension_fill_value,
    new_dimension_col_name,
    new_dimension_fill_value,
    attributes_to_move,
):
    columns = df.columns + [new_dimension_col_name]

    df_attributes_to_keep = df.filter(~F.col(existing_dimension_col_name).isin(attributes_to_move))
    df_attributes_to_keep = (df_attributes_to_keep
        .withColumn(new_dimension_col_name, F.lit(new_dimension_fill_value))
        .select(*columns)
    )

    df_attributes_to_move = df.filter(F.col(existing_dimension_col_name).isin(attributes_to_move))
    df_attributes_to_move = (df_attributes_to_move
        .withColumn(new_dimension_col_name, F.col(existing_dimension_col_name))
        .withColumn(existing_dimension_col_name, F.lit(existing_dimension_fill_value))
        .select(*columns)
    )

    df_updated = df_attributes_to_keep.union(df_attributes_to_move)

    return df_updated


def get_dimension_list_from_col(df, dimension_col_name):

    df_unique_dimensions = (df
        .select(dimension_col_name)
        .distinct()
    )

    dimension_cols = [
        row[dimension_col_name] 
        for row in df_unique_dimensions.select(dimension_col_name).collect()
    ]

    return dimension_cols
