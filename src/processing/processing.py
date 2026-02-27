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
    source_col_name,
    source_col_fill_value,
    new_col_name,
    new_col_fill_value,
    attributes_to_move,
):
    columns = df.columns + [new_col_name]

    df_attributes_to_keep = df.filter(~F.col(source_col_name).isin(attributes_to_move))
    df_attributes_to_keep = (df_attributes_to_keep
        .withColumn(new_col_name, F.lit(new_col_fill_value))
        .select(*columns)
    )

    df_attributes_to_move = df.filter(F.col(source_col_name).isin(attributes_to_move))
    df_attributes_to_move = (df_attributes_to_move
        .withColumn(new_col_name, F.col(source_col_name))
        .withColumn(source_col_name, F.lit(source_col_fill_value))
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


@register
def replace_col_values(df, value_mappings, col_name):

    df = df.replace(value_mappings, subset=[col_name])

    return df