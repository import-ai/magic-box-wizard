from typing import Annotated

from fastapi import Header
from sqlalchemy.ext.asyncio import AsyncSession

from wizard.common.trace_info import TraceInfo
from wizard.db import session_context


def get_trace_info(trace_id: Annotated[str | None, Header()] = None) -> TraceInfo:
    return TraceInfo(trace_id=trace_id)

async def get_session() -> AsyncSession:
    async with session_context() as session:
        yield session