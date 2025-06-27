class ProcessPipeError(Exception):
    """Base class for pipeline errors."""
    def __init__(self, message: str = "", *, code: str | None = None, params: dict | None = None) -> None:
        super().__init__(message)
        self.code = code or self.__class__.__name__
        self.params = params or {}


class JoinError(ProcessPipeError):
    pass


class UnionError(ProcessPipeError):
    pass


class AggregationError(ProcessPipeError):
    pass


class GroupSizeError(ProcessPipeError):
    pass


class FilterError(ProcessPipeError):
    pass


class RollingAggError(ProcessPipeError):
    pass


class SortError(ProcessPipeError):
    pass


class TopNError(ProcessPipeError):
    pass


class FillNAError(ProcessPipeError):
    pass


class RenameError(ProcessPipeError):
    pass


class CastError(ProcessPipeError):
    pass


class StringError(ProcessPipeError):
    pass


class DropDuplicateError(ProcessPipeError):
    pass


class PartitionAggError(ProcessPipeError):
    pass


class RowNumberError(ProcessPipeError):
    pass


class DeleteError(ProcessPipeError):
    pass


class UpdateError(ProcessPipeError):
    pass


class CaseError(ProcessPipeError):
    pass
