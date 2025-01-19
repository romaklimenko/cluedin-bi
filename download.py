# pylint: disable=missing-module-docstring disable=missing-function-docstring
# This script downloads data from CluedIn using the GraphQL API
# and saves it to a parquet file.
import os

import cluedin
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

ctx = cluedin.Context.from_jwt(ACCESS_TOKEN)

print(ctx)

with open('graphql/entities.gql', 'r', encoding='utf-8') as f:
    query = f.read()

df = pd.DataFrame(
    cluedin.gql.entries(
        ctx,
        query,
        {'query': '*', 'pageSize': 10_000},
        flat=True),
    dtype=str)

print(df.head())


df.set_index('id', inplace=True)
df.reset_index(inplace=True)

first_cols = [
    'id',
    'name',
    'entityType',
    'discoveryDate',
    'createdDate',
    'modifiedDate',
    'codes',
    'tags'
]
df = df[
    first_cols +
    sorted([col for col in df.columns if col not in first_cols])]


PATH = 'fixtures/cluedin-{}.parquet'
i = 0
while os.path.exists(PATH.format(i)):
    i += 1

df.to_parquet(PATH.format(i))
