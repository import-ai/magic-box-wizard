from logging import Logger
from typing import Optional

import shortuuid

from worker.common.logger import get_logger


class TraceInfo:
    def __init__(self, trace_id: Optional[str] = None, logger: Optional[Logger] = None, payload: Optional[dict] = None):
        self.trace_id = trace_id or shortuuid.uuid()
        self.logger = logger or get_logger("app")
        if self.trace_id not in self.logger.name.split("."):
            self.logger = self.logger.getChild(self.trace_id)
        self.payload: dict = payload or {}

    def get_child(self, name: str = None, addition_payload: Optional[dict] = None) -> "TraceInfo":
        return self.__class__(
            self.trace_id,
            self.logger if name is None else self.logger.getChild(name),
            self.payload | (addition_payload or {})
        )
