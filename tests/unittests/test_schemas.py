import pytest
from pyspark.sql.types import StringType, TimestampType

from src.schemas.dimensions import DIMENSIONS_SCHEMA, create_dimensions_schema


DIMENSIONS = ["AgeAtBookingMotherAvg", "BirthweightTermGroup", "TotalDeliveries"]


def test_create_dimensions_schema_contains_base_fields():
    schema = create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    assert "dimension_cohort_id" in field_names
    assert "metric_id" in field_names


def test_create_dimensions_schema_contains_dimension_fields():
    schema = create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    for col in DIMENSIONS:
        assert col in field_names


def test_create_dimensions_schema_base_fields_come_first():
    schema = create_dimensions_schema(DIMENSIONS)
    field_names = [f.name for f in schema.fields]

    base_field_names = [f.name for f in DIMENSIONS_SCHEMA.fields]
    assert field_names[:len(base_field_names)] == base_field_names


def test_create_dimensions_schema_dimension_fields_are_nullable_strings():
    schema = create_dimensions_schema(DIMENSIONS)
    dimension_fields = {f.name: f for f in schema.fields}

    for col in DIMENSIONS:
        assert isinstance(dimension_fields[col].dataType, StringType)
        assert dimension_fields[col].nullable is True


def test_create_dimensions_schema_does_not_mutate_base_schema():
    """
    test to make sure that create_dimensions_schema(DIMENSIONS) does
    not accidentally mutate DIMENSIONS_SCHEMA in-place.
    """
    before = [f.name for f in DIMENSIONS_SCHEMA.fields]
    create_dimensions_schema(DIMENSIONS)
    after = [f.name for f in DIMENSIONS_SCHEMA.fields]

    assert before == after
