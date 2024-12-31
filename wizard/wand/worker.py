import asyncio
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func, desc, asc
from sqlalchemy.exc import IntegrityError

from common.exception import CommonException
from common.logger import get_logger
from wizard.config import Config
from wizard.db import get_session_factory
from wizard.db.entity import Task as ORMTask
from wizard.entity import Task
from wizard.wand.functions.base_function import BaseFunction
from wizard.wand.functions.html_to_markdown import HTMLToMarkdown
from wizard.wand.functions.index import CreateOrUpdateIndex, DeleteIndex


class Worker:
    def __init__(self, config: Config, worker_id: int):
        self.worker_id = worker_id

        self.function_dict: dict[str, BaseFunction] = {
            "create_or_update_index": CreateOrUpdateIndex(config),
            "delete_index": DeleteIndex(config),
            "html_to_markdown": HTMLToMarkdown()
        }

        self.logger = get_logger("worker")
        self.session_factory = get_session_factory(config.db.url)

    async def run(self):
        while True:
            task: Task = await self.fetch_and_claim_task()
            if task:
                self.logger.info(
                    {
                        "worker_id": self.worker_id,
                        "namespace_id": task.namespace_id,
                    } | task.model_dump(include={"task_id", "created_at", "started_at"})
                )
                await self.process_task(task)
            else:
                self.logger.debug({
                    "worker_id": self.worker_id,
                    "message": "No available task, waiting..."
                })
                await asyncio.sleep(1)

    async def fetch_and_claim_task(self) -> Optional[Task]:
        task: Optional[Task] = None
        async with self.session_factory() as session:
            try:
                async with session.begin():
                    # Subquery to count running tasks per user
                    running_tasks_sub_query = (
                        select(
                            ORMTask.namespace_id,
                            func.count(ORMTask.task_id).label('running_count')
                        )
                        .where(ORMTask.started_at != None, ORMTask.ended_at == None, ORMTask.canceled_at == None)
                        .group_by(ORMTask.namespace_id)
                        .subquery()
                    )

                    # Subquery to find one eligible task_id that can be started
                    task_id_subquery = (
                        select(ORMTask.task_id)
                        .outerjoin(running_tasks_sub_query,
                                   ORMTask.namespace_id == running_tasks_sub_query.c.namespace_id)
                        .where(ORMTask.started_at == None)
                        .where(ORMTask.canceled_at == None)
                        .where(
                            func.coalesce(running_tasks_sub_query.c.running_count, 0) < ORMTask.concurrency_threshold)
                        .order_by(desc(ORMTask.priority), asc(ORMTask.created_at))
                        .limit(1)
                        .subquery()
                    )

                    # Actual query to lock the task row
                    stmt = (
                        select(ORMTask)
                        .where(ORMTask.task_id.in_(select(task_id_subquery.c.task_id)))
                        .with_for_update(skip_locked=True)
                    )

                    result = await session.execute(stmt)
                    orm_task = result.scalars().first()

                    if orm_task:
                        # Mark the task as started
                        orm_task.started_at = datetime.now()
                        session.add(orm_task)
                        task = Task.model_validate(orm_task)
                        await session.commit()
            except IntegrityError:  # Handle cases where the task was claimed by another worker
                await session.rollback()
            except Exception as e:
                self.logger.exception({
                    "worker_id": self.worker_id,
                    "error": CommonException.parse_exception(e)
                })
                await session.rollback()
            return task

    async def process_task(self, task: Task):
        try:
            # Placeholder for actual processing logic
            output = await self.worker_router(task)

            # Update the task with the result
            async with self.session_factory() as session:
                async with session.begin():
                    orm_task = await session.get(ORMTask, task.task_id)
                    orm_task.output = output
                    orm_task.ended_at = datetime.now()
                    session.add(orm_task)
                    task = Task.model_validate(orm_task)
                    await session.commit()
            self.logger.info(
                {
                    "worker_id": self.worker_id,
                } | task.model_dump(include={"task_id", "created_at", "started_at", "ended_at"}))
        except Exception as e:
            # Update the task with the exception details
            async with self.session_factory() as session:
                async with session.begin():
                    orm_task = await session.get(ORMTask, task.task_id)
                    orm_task.exception = {"error": CommonException.parse_exception(e)}
                    orm_task.ended_at = datetime.now()
                    session.add(orm_task)
                    await session.commit()

            self.logger.exception(
                {
                    "worker_id": self.worker_id,
                    "error": CommonException.parse_exception(e)
                } | task.model_dump(include={"task_id", "created_at", "started_at", "ended_at"})
            )

    async def worker_router(self, task: Task) -> dict:
        function = task.function
        if function in self.function_dict:
            worker = self.function_dict[function]
            return await worker.run(task)
        raise ValueError(f"Invalid function: {function}")
