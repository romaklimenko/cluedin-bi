# pylint: disable=missing-module-docstring missing-function-docstring

import pandas as pd
from deltalake import write_deltalake


def merge_table(
    source_df: pd.DataFrame,
    target_delta_path: str,
) -> None:
    """
    Merge source_df into target_delta_path.
    """
    write_deltalake(
        table_or_uri=target_delta_path,
        data=source_df,
        mode="overwrite",
        schema_mode="merge",
    )
