"""Shared pydantic config and base models."""

# Standard library imports
from logging import getLogger
from typing import Any, Self

# Third party imports
from pydantic import (
  BaseModel,
  ConfigDict,
  ModelWrapValidatorHandler,
  RootModel,
  SerializationInfo,
  SerializerFunctionWrapHandler,
  ValidationError,
  ValidationInfo,
  ValidatorFunctionWrapHandler,
  field_validator,
  model_serializer,
  model_validator,
)
from pydantic_core import from_json

logger = getLogger(__name__)


PYDANTIC_CONFIG = ConfigDict(
  populate_by_name=True,
  use_enum_values=True,
  validate_default=True,
  validate_assignment=True,
  coerce_numbers_to_str=True,
)


class CustomRootModel[T](RootModel[T]):
  """RootModel that also accepts a JSON string and serializes to one in JSON mode."""

  model_config = PYDANTIC_CONFIG
  _dumping_json: bool = False

  # @model_serializer(mode="wrap", when_used="json")
  # def serialize_as_jsonstr(self, nxt: SerializerFunctionWrapHandler):
  #   if not self._dumping_json:
  #     self._dumping_json = True
  #     return self.model_dump_json()
  #   else:
  #     self._dumping_json = False
  #     return nxt(self)

  @model_validator(mode="wrap")
  @classmethod
  def validate_as_jsonstr(cls, data: Any, handler: ValidatorFunctionWrapHandler) -> Self:
    """Parse *data* from JSON when it arrives as a string, then validate normally."""
    if isinstance(data, str):
      data = from_json(data)
    return handler(data)

  @model_serializer(mode="wrap", when_used="unless-none")
  def return_self(self, nxt: SerializerFunctionWrapHandler, info: SerializationInfo):
    """In JSON mode dump the root as a JSON string; `_dumping_json` breaks the recursion `model_dump_json` causes."""
    if not info.mode_is_json():
      return self
    if not self._dumping_json:
      self._dumping_json = True
      return self.model_dump_json()
    else:
      self._dumping_json = False
      return nxt(self)


class CustomBaseModel(BaseModel):
  """BaseModel with the shared config and validation hooks."""

  model_config = PYDANTIC_CONFIG

  @field_validator("*", mode="wrap", check_fields=False)
  @classmethod
  def log_failed_field_validations(cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
    """Run the field validator, falling back to the raw *data* when it yields None."""
    results = None

    results = handler(data)

    return data if results is None else results

  @model_validator(mode="wrap")
  @classmethod
  def log_failed_validation(cls, data: Any, handler: ModelWrapValidatorHandler[Self], info: ValidationInfo) -> Self:
    """Run model validation; for `ScheduledOrderDBEntryModel` only, tolerate missing-field errors and return None."""
    results = None
    try:
      results = handler(data)
    except ValidationError as e:
      if cls.__name__ != "ScheduledOrderDBEntryModel":
        raise

      for err in e.errors():
        if err["type"] != "missing":
          raise
    return results  # type: ignore
