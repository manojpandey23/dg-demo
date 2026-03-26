from datetime import datetime
from typing import Iterable

import pandas as pd

from framework.cdc.model.events import ChangeEvent


def compute_changes(
    *,
    asset_name: str,
    new_df: pd.DataFrame,
    old_df: pd.DataFrame | None,
    primary_key: list[str],
    run_id: str,
) -> list[ChangeEvent]:

    ts = datetime.utcnow()

    if old_df is None:
        return [
            ChangeEvent(
                asset=asset_name,
                op="INSERT",
                pk=row[primary_key].to_dict(),
                before=None,
                after=row.to_dict(),
                run_id=run_id,
                ts=ts,
            )
            for _, row in new_df.iterrows()
        ]

    new = new_df.set_index(primary_key)
    old = old_df.set_index(primary_key)

    events: list[ChangeEvent] = []

    # INSERTS
    for pk, row in new.loc[new.index.difference(old.index)].iterrows():
        events.append(
            ChangeEvent(
                asset=asset_name,
                op="INSERT",
                pk=dict(zip(primary_key, pk if isinstance(pk, tuple) else [pk])),
                before=None,
                after=row.to_dict(),
                run_id=run_id,
                ts=ts,
            )
        )

    # DELETES
    for pk, row in old.loc[old.index.difference(new.index)].iterrows():
        events.append(
            ChangeEvent(
                asset=asset_name,
                op="DELETE",
                pk=dict(zip(primary_key, pk if isinstance(pk, tuple) else [pk])),
                before=row.to_dict(),
                after=None,
                run_id=run_id,
                ts=ts,
            )
        )

    # UPDATES
    common = new.index.intersection(old.index)
    for pk in common:
        if not new.loc[pk].equals(old.loc[pk]):
            events.append(
                ChangeEvent(
                    asset=asset_name,
                    op="UPDATE",
                    pk=dict(zip(primary_key, pk if isinstance(pk, tuple) else [pk])),
                    before=old.loc[pk].to_dict(),
                    after=new.loc[pk].to_dict(),
                    run_id=run_id,
                    ts=ts,
                )
            )

    return events
