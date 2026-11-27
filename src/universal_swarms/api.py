"""FastAPI application for swarm sessions and committed-event streaming."""
import asyncio
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

import psycopg
import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from .config import load_profile
from .llm import LlmClient
from .runtime import Engine
from .store import AdmissionConflict, Busy, RunNotFound, Store, dump

logger = logging.getLogger(__name__)
RunStatus = Literal["active", "complete", "blocked"]


class SessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    config_ref: Annotated[str, Field(max_length=128, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.yaml$")] | None = None
    user_prompt: Annotated[str, Field(min_length=1, max_length=32768)] | None = None


class Session(BaseModel):
    id: UUID
    status: RunStatus
    config_ref: str


class SessionCreated(BaseModel):
    session: Session


class WorkflowState(BaseModel):
    completed_phases: list[str]


class SessionState(BaseModel):
    run_id: UUID
    status: RunStatus
    run_status: RunStatus
    reason: str | None
    final_answer: str | None
    memory: list[dict[str, Any]]
    normalizations: list[dict[str, Any]] = Field(default_factory=list)
    workflow: WorkflowState


def create_app(
    dsn: str,
    model: Callable[[], LlmClient],
    default_profile: Path,
    *,
    model_name: str | None = None,
    max_schedulers: int = 8,
) -> FastAPI:
    """Create an ASGI app with one tracked task per run in this process."""
    tasks: dict[UUID, asyncio.Task] = {}
    profile_dir = default_profile.parent.resolve()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    app = FastAPI(title="Universal Swarms", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(RunNotFound)
    async def not_found(_: Request, error: RunNotFound) -> JSONResponse:
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(AdmissionConflict)
    async def conflict(_: Request, error: AdmissionConflict) -> JSONResponse:
        return JSONResponse({"detail": str(error)}, status_code=status.HTTP_409_CONFLICT)

    @app.exception_handler(psycopg.Error)
    async def database_error(_: Request, error: psycopg.Error) -> JSONResponse:
        logger.error("Swarm database request failed (%s)", type(error).__name__)
        return JSONResponse({"detail": "Swarm database unavailable"}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    def inspect(run_id: UUID) -> dict:
        with Store(dsn) as store:
            return store.inspect(run_id)

    @app.post("/sessions", response_model=SessionCreated, status_code=status.HTTP_201_CREATED)
    def create_session(
        body: SessionRequest,
        idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255, pattern=r"^[!-~]+$")] = None,
    ) -> SessionCreated:
        ref = body.config_ref or default_profile.name
        profile = (profile_dir / ref).resolve()
        if profile.parent != profile_dir or not profile.is_file():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown swarm profile")
        try:
            config = load_profile(profile)
        except (OSError, ValueError, TypeError, KeyError, yaml.YAMLError) as error:
            logger.error("Could not load swarm profile %s (%s)", ref, type(error).__name__)
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Swarm profile is invalid") from None
        goal = body.user_prompt if body.user_prompt is not None else config.get("goal")
        if not isinstance(goal, str) or not goal.strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "A goal is required")
        config.setdefault("llm", {})["model"] = model_name or os.environ["MAIN_MODEL"]
        with Store(dsn) as store:
            run_id = store.admit(config, goal, idempotency_key or str(uuid4()))
            state = store.inspect(run_id)
        return SessionCreated(session=Session(id=run_id, status=state["status"], config_ref=ref))

    @app.get("/sessions/{run_id}", response_model=SessionState)
    def get_session(run_id: UUID) -> dict:
        return inspect(run_id)

    def execute(run_id: UUID) -> None:
        with Store(dsn) as store:
            asyncio.run(Engine(store, model(), run_id).run_all())

    def events(run_id: UUID, after: int) -> list[dict]:
        with Store(dsn) as store:
            return store.events(run_id, after)

    async def run_session(run_id: UUID) -> bool:
        try:
            await run_in_threadpool(execute, run_id)
        except Busy:
            return True
        except Exception as error:
            logger.error("Scheduler failed for run %s (%s)", run_id, type(error).__name__)
            return False
        return True

    @app.post("/sessions/{run_id}/run/stream", response_class=StreamingResponse,
              responses={200: {"content": {"text/event-stream": {}}}})
    async def stream_session(
        run_id: UUID,
        last_event_id: Annotated[int, Header(ge=0)] = 0,
    ) -> StreamingResponse:
        state = await run_in_threadpool(inspect, run_id)
        task = tasks.get(run_id)
        if task is None and state["status"] == "active":
            if len(tasks) >= max_schedulers:
                raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Scheduler capacity reached",
                                    headers={"Retry-After": "1"})
            task = asyncio.create_task(run_session(run_id))
            tasks[run_id] = task
            task.add_done_callback(lambda _: tasks.pop(run_id, None))

        async def stream() -> AsyncIterator[str]:
            after = last_event_id
            loop = asyncio.get_running_loop()
            heartbeat = loop.time() + 15
            try:
                while True:
                    done = task is None or task.done()
                    for event in await run_in_threadpool(events, run_id, after):
                        after = event["sequence"]
                        yield f"id: {after}\nevent: {event['kind']}\ndata: {dump(event)}\n\n"
                    if done:
                        if task is not None and not task.result():
                            yield 'event: error\ndata: {"error":"Scheduler failed; inspect the run ledger"}\n\n'
                        else:
                            observed = await run_in_threadpool(inspect, run_id)
                            yield f"event: state\ndata: {dump(observed)}\n\n"
                        return
                    if loop.time() >= heartbeat:
                        yield ": keep-alive\n\n"
                        heartbeat = loop.time() + 15
                    await asyncio.sleep(0.25)
            except psycopg.Error as error:
                logger.error("Ledger stream failed for run %s (%s)", run_id, type(error).__name__)
                yield 'event: error\ndata: {"error":"Swarm database unavailable"}\n\n'

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


def serve(dsn: str, model: Callable[[], LlmClient], default_profile: Path, port: int) -> None:
    app = create_app(dsn, model, default_profile, model_name=os.environ["MAIN_MODEL"])
    uvicorn.run(app, host="127.0.0.1", port=port)
