from pyspark.sql import functions as F
import sys


def create_dimension_columns(
    df,
    dimension_col_name,
    attribute_col_name,
    dimensions,
    dimensions_to_exclude
):
    def _create_new_dim_col_expr(dimension):
        return (   
            F.when(
                (F.col(dimension_col_name) == dimension)
                & 
                (F.col(attribute_col_name).isNull()), 
                F.concat(F.lit("all_"), F.lit(dimension))
             )
             .when(
                F.col(dimension_col_name) == dimension, 
                F.col(attribute_col_name)
             )
             .otherwise(F.concat(F.lit("all_"), F.lit(dimension) ))
             .alias(dimension)
        )

    new_dimension_cols = []
    for dimension in dimensions:
        if dimension in dimensions_to_exclude:
            continue
        new_dimension_cols.append(
            _create_new_dim_col_expr(dimension)
        )

    df = df.select("*", *new_dimension_cols)

    return df


def create_dimension_cohort_id_col(df, dimension_cols):
    
    df = df.withColumn(
        "dimension_cohort_id",
        F.concat_ws(
            "|",
            *[F.col(col) for col in dimension_cols]
        )
    )

    return df


def create_dimension_table(df, dimension_cols, dimensions_to_exclude, dimension_col_name="Dimension", attribute_col_name="Measure"):
    df = create_dimension_columns(
        df,
        dimension_col_name,
        attribute_col_name,
        dimension_cols,
        dimensions_to_exclude
    )

    df = create_dimension_cohort_id_col(
        df,
        dimension_cols
    )

    return df


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


def create_md5_hash_col(df, cols, new_col_name):
    df = df.withColumn(
        new_col_name,
        F.md5(F.concat_ws("|", *[F.col(c) for c in cols]))
    )
    return df
