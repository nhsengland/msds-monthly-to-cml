from pyspark.sql import functions as F
from pyspark import sql as pyspark

PROCESSING_FUNC_REGISTRY = {}

def register(func):
    """
    A decorator used to register a processing function.
    """
    PROCESSING_FUNC_REGISTRY[func.__name__] = func
    return func


@register
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


@register
def rename_cols(df, col_name_mappings):

    current_cols = df.columns
    new_cols = []
    for current_col_name in current_cols:
        if current_col_name not in col_name_mappings:
            new_cols.append(current_col_name)
        else:
            new_cols.append(
                F.col(current_col_name).alias(col_name_mappings[current_col_name])
            )

    return df.select(*new_cols)

