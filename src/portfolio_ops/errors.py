"""实验样本事件快处服务向 API 和 CLI 暴露的稳定错误。"""


class CollectionDispatchError(RuntimeError):
    code = "traffic_error"
    status = 400


class NotFound(CollectionDispatchError):
    code = "not_found"
    status = 404


class Conflict(CollectionDispatchError):
    code = "conflict"
    status = 409


class IdempotencyConflict(Conflict):
    code = "idempotency_conflict"

    def __init__(self, message: str, *, conflict_id: int | None = None) -> None:
        super().__init__(message)
        self.conflict_id = conflict_id


class Forbidden(CollectionDispatchError):
    code = "forbidden"
    status = 403


class InvalidState(CollectionDispatchError):
    code = "invalid_state"
    status = 409


class ValidationFailed(CollectionDispatchError):
    code = "validation_failed"
    status = 422
