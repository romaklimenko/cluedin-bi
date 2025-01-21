# pylint: disable=missing-module-docstring missing-function-docstring

# TODO: calculate metrics and fill Fact_DataQuality table

import os
import shutil

import pandas as pd

from schema import (ensure_dim_date, ensure_dim_entity_type, ensure_dim_tags,
                    ensure_fact)

# delete all parquet tables from `data/` folder
DATA_FOLDER = 'data/'

for filename in os.listdir(DATA_FOLDER):
    file_path = os.path.join(DATA_FOLDER, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    # pylint: disable=broad-except
    except Exception as e:
        print(f'Failed to delete {file_path}. Reason: {e}')


def prepare_entities(source_parquet_path: str) -> pd.DataFrame:
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


for file in sorted(os.listdir('fixtures')):
    if file.endswith('.parquet'):
        print(file)
        ensure_fact(
            prepare_entities(f'fixtures/{file}'),
            'data/Fact_Entities.parquet')

        fact_entities_df = pd.read_parquet('data/Fact_Entities.parquet')

        ensure_dim_entity_type(
            fact_entities_df,
            'data/Dim_EntityType.parquet')

        ensure_dim_tags(
            fact_entities_df,
            'data/Dim_Tag.parquet')

        ensure_dim_date(
            'data/Dim_Date.parquet')
