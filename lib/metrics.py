# pylint: disable=missing-module-docstring missing-function-docstring
import pandas as pd

from lib.dataclasses import FactDataQuality


def count_entities_by_type(fact_entities_df, now=None) -> list[FactDataQuality]:
    if now is None:
        now = pd.Timestamp.now().strftime('%Y-%m-%d')

    count_by_type = fact_entities_df['entityType'].value_counts().reset_index()
    count_by_type.columns = ['entityType', 'count']

    metric_key = 'entities.count'

    return [
        FactDataQuality(
            Date_Key=now,
            EntityType_Key=entity_type,
            Property_Key=None,
            Tag_Key=None,
            Metric_Key=metric_key,
            Value=count
        )
        for entity_type, count in count_by_type.values
    ]


def count_entities_by_tag(fact_entities_df, now=None) -> list[FactDataQuality]:
    if now is None:
        now = pd.Timestamp.now().strftime('%Y-%m-%d')

    tags_df = fact_entities_df.explode('tags')
    count_by_tag = tags_df['tags'].value_counts().reset_index()
    count_by_tag.columns = ['tags', 'count']

    metric_key = 'entities.count'

    return [
        FactDataQuality(
            Date_Key=now,
            EntityType_Key=None,
            Property_Key=None,
            Tag_Key=tag,
            Metric_Key=metric_key,
            Value=count
        )
        for tag, count in count_by_tag.values
    ]


def calculate_uniqueness_by_entity_type_and_property(
    fact_entities_df, now=None
) -> list[FactDataQuality]:
    if now is None:
        now = pd.Timestamp.now().strftime('%Y-%m-%d')

    excluded_columns = [
        'id',
        'entityType',
        'discoveryDate',
        'createdDate',
        'modifiedDate',
        'tags',
        'codes'
    ]

    entity_type_property_pairs = [
        (entity_type, property_key)
        for entity_type in fact_entities_df['entityType'].unique()
        for property_key in fact_entities_df.columns
        if property_key not in excluded_columns
    ]

    metric_key = 'properties.uniqueness'

    return [
        FactDataQuality(
            Date_Key=now,
            EntityType_Key=entity_type,
            Property_Key=property_key,
            Tag_Key=None,
            Metric_Key=metric_key,
            Value=fact_entities_df.dropna(subset=[property_key]).groupby(
                property_key).ngroups / fact_entities_df.dropna(subset=[property_key]).shape[0]
        )
        for entity_type, property_key in entity_type_property_pairs
    ]
