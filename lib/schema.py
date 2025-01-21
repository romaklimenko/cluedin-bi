# pylint: disable=missing-module-docstring missing-function-docstring

import os

import pandas as pd


def ensure_fact(
    source_df: pd.DataFrame,
    target_parquet_path: str,
) -> None:
    if 'Key' in source_df.columns:
        source_df = source_df[['Key'] +
                              [col for col in source_df.columns if col != 'Key']]
    source_df.to_parquet(
        target_parquet_path,
        index=False,
        compression='snappy'
    )


def ensure_dim_entity_type(fact_entities_df, path_to_table):
    dim_df = fact_entities_df['entityType'].to_frame().drop_duplicates()
    dim_df.columns = ['Key']
    dim_df['EntityType'] = dim_df['Key'].replace(
        '/', ' ', regex=True).str.strip()
    merge_dim_df(dim_df, path_to_table)


def ensure_dim_tags(fact_entities_df, path_to_table):
    tags_df = fact_entities_df \
        .explode('tags')['tags'] \
        .to_frame() \
        .drop_duplicates() \
        .query('tags != ""')
    tags_df.columns = ['Key']
    tags_df['Tag'] = tags_df['Key'].replace('T:', '', regex=True)
    merge_dim_df(tags_df, path_to_table)


def ensure_dim_date(pass_to_table):
    current_year = pd.Timestamp.now().year

    date_range = pd.date_range(
        f'{current_year - 5}-01-01',
        f'{current_year}-12-31')

    dim_df = pd.DataFrame({
        'Key': date_range.strftime('%Y-%m-%d'),
        'Month': date_range.strftime('%Y-%m'),
        'Year': date_range.strftime('%Y')
    })
    merge_dim_df(dim_df, pass_to_table)


def ensure_dim_metric(fact_data_quality_df, path_to_table):
    dim_df = fact_data_quality_df['Metric_Key'].to_frame().drop_duplicates()
    dim_df.columns = ['Key']
    dim_df['Metric'] = dim_df['Key'] \
        .str.replace('\\.', ' ', regex=True).str.title()
    merge_dim_df(dim_df, path_to_table)


def merge_dim_df(dim_df, path_to_table):
    if os.path.exists(path_to_table):
        existing_df = pd.read_parquet(path_to_table)
        dim_df = pd \
            .concat([existing_df, dim_df]) \
            .drop_duplicates()

    dim_df.to_parquet(
        path_to_table,
        index=False,
        compression='snappy')


def append_fact(fact_df, path_to_table):
    if 'Key' in fact_df.columns:
        fact_df = fact_df[
            ['Key'] + [col for col in fact_df.columns if col != 'Key']]

    if os.path.exists(path_to_table):
        existing_df = pd.read_parquet(path_to_table)
        fact_df = pd.concat([existing_df, fact_df]).drop_duplicates(
            subset='Key', keep='last')

    fact_df.to_parquet(
        path_to_table,
        index=False,
        compression='snappy'
    )
