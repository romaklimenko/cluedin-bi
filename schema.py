# pylint: disable=missing-module-docstring missing-function-docstring

import os

import pandas as pd


def ensure_fact(
    source_df: pd.DataFrame,
    target_parquet_path: str,
) -> None:
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
    save_dim_df(dim_df, path_to_table)


def ensure_dim_tags(fact_entities_df, path_to_table):
    tags_df = fact_entities_df \
        .explode('tags')['tags'] \
        .to_frame() \
        .drop_duplicates() \
        .query('tags != ""')
    tags_df.columns = ['Key']
    tags_df['Tag'] = tags_df['Key'].replace('T:', '', regex=True)
    save_dim_df(tags_df, path_to_table)


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
    save_dim_df(dim_df, pass_to_table)


def save_dim_df(dim_df, path_to_table):
    if os.path.exists(path_to_table):
        existing_df = pd.read_parquet(path_to_table)
        dim_df = pd \
            .concat([existing_df, dim_df]) \
            .drop_duplicates()

    dim_df.to_parquet(
        path_to_table,
        index=False,
        compression='snappy')


def ensure_dim(
    fact_df: pd.DataFrame,
    dim_path: str,
    columns: list[str],
) -> None:
    """
    Create Dim table from Fact table.
    """

    dim_df = fact_df[columns].drop_duplicates()
    dim_df.columns = ['Key'] + columns[1:]

    if os.path.exists(dim_path):
        existing_df = pd.read_parquet(dim_path)
        dim_df = pd.concat([existing_df, dim_df])
        # drop duplicates
        dim_df = dim_df.drop_duplicates()

    dim_df.to_parquet(
        dim_path,
        index=False,
        compression='snappy')
