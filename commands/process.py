# pylint: disable=missing-module-docstring missing-function-docstring

import os
import shutil

import pandas as pd

from lib.metrics import (calculate_uniqueness_by_entity_type_and_property,
                         count_entities_by_tag, count_entities_by_type)
from lib.schema import (append_fact, ensure_dim_date, ensure_dim_entity_type,
                        ensure_dim_metric, ensure_dim_tags, ensure_fact)


def _prepare_entities(source_parquet_path: str) -> pd.DataFrame:
    """
    Prepare entities table for merging into Fact_Entities table.
    Put any transformations here.
    For example, convert string to datetime or integer, or split string into list.
    """
    entities_df = pd.read_parquet(source_parquet_path)
    entities_df.set_index('id', inplace=True)

    entities_df['discoveryDate'] = pd.to_datetime(
        entities_df['discoveryDate']) \
        .dt.ceil('us') \
        .dt.strftime('%Y-%m-%d')
    entities_df['createdDate'] = pd.to_datetime(
        entities_df['createdDate']) \
        .dt.ceil('us') \
        .dt.strftime('%Y-%m-%d')
    entities_df['modifiedDate'] = pd.to_datetime(
        entities_df['modifiedDate']) \
        .dt.ceil('us') \
        .dt.strftime('%Y-%m-%d')

    # I forgot to fix this when generating the data
    mask = entities_df['createdDate'] > entities_df['modifiedDate']
    entities_df.loc[mask, ['createdDate', 'modifiedDate']] = \
        entities_df.loc[mask, ['modifiedDate', 'createdDate']].values

    def split_by_comma(x):
        return x.strip('[]').replace("'", "").split(', ')

    entities_df['tags'] = entities_df['tags'].apply(split_by_comma)
    entities_df['codes'] = entities_df['codes'].apply(split_by_comma)

    return entities_df


def process():

    # delete all parquet tables from `data/` folder
    data_folder = 'data/'

    for filename in os.listdir(data_folder):
        file_path = os.path.join(data_folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        # pylint: disable=broad-except
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

    fixtures = sorted(os.listdir('fixtures'))
    for i, file in enumerate(fixtures):
        if file.endswith('.parquet'):
            print(file)

            # Fact_Entities

            ensure_fact(
                _prepare_entities(f'fixtures/{file}'),
                'data/Fact_Entities.parquet')

            fact_entities_df = pd.read_parquet('data/Fact_Entities.parquet')

            # Dim tables from Fact_Entities

            ensure_dim_entity_type(
                fact_entities_df,
                'data/Dim_EntityType.parquet')

            ensure_dim_tags(
                fact_entities_df,
                'data/Dim_Tag.parquet')

            ensure_dim_date(
                'data/Dim_Date.parquet')

            # Fact_DataQuality

            fake_now = (pd.Timestamp.now() - pd.Timedelta(days=len(fixtures) - i)) \
                .strftime('%Y-%m-%d')

            data_quality_df = pd.DataFrame(
                count_entities_by_type(fact_entities_df, fake_now) +
                count_entities_by_tag(fact_entities_df, fake_now) +
                calculate_uniqueness_by_entity_type_and_property(
                    fact_entities_df, fake_now)
            )

            append_fact(
                data_quality_df,
                'data/Fact_DataQuality.parquet')

            # Dim_Metric

            fact_data_quality_df = pd.read_parquet(
                'data/Fact_DataQuality.parquet')

            ensure_dim_metric(
                fact_data_quality_df,
                'data/Dim_Metric.parquet')
