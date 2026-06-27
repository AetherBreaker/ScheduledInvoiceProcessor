# Standard library imports
from logging import getLogger
from typing import TYPE_CHECKING

# Third party imports
from numpy import nan
from pandas import DataFrame, Series, concat, isna

# First party imports
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns

# Local folder imports
from .models.db_entries import ScheduledOrderDBEntryModel, ScheduleValidationError

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Sequence

  # First party imports
  from scheduled_invoice_processor.typing_custom.dataframe_column_names import ColNameEnum

  # Local folder imports
  from . import CustomBaseModel

logger = getLogger(__name__)

NULL_VALUES = ["NULL", "", " ", float("nan")]


def build_typed_dataframe(
  data: Sequence[Sequence[str | int | float | None]], columns: type[ColNameEnum], types_model: type[CustomBaseModel]
) -> DataFrame:
  # pad the data with columns of None to match the number of expected columns
  data = [[row[idx] if idx < len(row) else nan for idx in range(len(columns.all_columns()))] for row in data]

  # initialize dataframe
  df = DataFrame(data, columns=columns.all_columns(), dtype=object)

  if not df.empty:
    # Ensure all None-like objects within the dataframe are replaced with None prior to validation
    df = df.infer_objects(copy=False).replace(NULL_VALUES, value=nan)

    newly_typed_rows: list[Series] = []

    df.apply(
      apply_model,
      axis=1,
      types_model=types_model,
      typed_rows=newly_typed_rows,
    )

    df = concat(
      newly_typed_rows,
      axis=1,
      ignore_index=False,
    ).T

    # Ensure columns are in the order defined in their column names enumeration
    df = df[columns.all_columns()]

  df = df.set_index(
    columns.__index_items__,
    drop=False,
    verify_integrity=True,
  )

  return df


def apply_model(row: Series, types_model: type[CustomBaseModel], typed_rows: list[Series]) -> Series:
  row_dict = {k: v for k, v in row.to_dict().items() if not isna(v) or v is None}
  model = types_model.model_validate(row_dict)

  if model is not None:  # pyright: ignore[reportUnnecessaryComparison]
    model_dict = model.model_dump()

    typed_rows.append(Series(model_dict, name=row.name, dtype=object))
  elif types_model is ScheduledOrderDBEntryModel:
    logger.error(
      f"Row with index {row.name} failed validation and could not be converted to ScheduledOrderDBEntryModel\n"
      + "\n".join(f"{k}: {v}" for k, v in row_dict.items())
    )
    raise ScheduleValidationError(
      f"Entry {row[DatabaseScheduleColumns.supplier]} SFT{row[DatabaseScheduleColumns.store]:0>3} failed validation and could not be converted to ScheduledOrderDBEntryModel",
      row,
      row_dict,
    )

  return row
