from typing import Any, Dict, List, Optional, Sequence, Union

import langsmith.client
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.exceptions import RequestValidationError
from langchain.pydantic_v1 import ValidationError
from langchain_core.messages import AnyMessage
from langchain_core.runnables import RunnableConfig
from langserve.schema import FeedbackCreateRequest
from langsmith.utils import tracing_is_enabled
from pydantic import BaseModel, Field
from sse_starlette import EventSourceResponse

from app.agent import agent, AvailableTools
from app.auth.handlers import AuthedUser
from app.storage import get_assistant, get_thread
from app.stream import astream_state, to_sse
from app.tools import get_code_interpreter

router = APIRouter()


class CreateRunPayload(BaseModel):
    """Payload for creating a run."""

    thread_id: str
    # input: Optional[Union[Sequence[AnyMessage], Dict[str, Any]]] = Field(
    input: Optional[List[Dict[str, Any]]] = Field(
        default_factory=dict
    )
    config: Optional[RunnableConfig] = None


async def _run_input_and_config(payload: CreateRunPayload, user_id: str):
    thread = await get_thread(user_id, payload.thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    assistant = await get_assistant(user_id, str(thread["assistant_id"]))
    if not assistant:
        raise HTTPException(status_code=404, detail="Assistant not found")

    config: RunnableConfig = {
        **assistant["config"],
        "configurable": {
            **assistant["config"]["configurable"],
            **((payload.config or {}).get("configurable") or {}),
            "user_id": user_id,
            "thread_id": str(thread["thread_id"]),
            "assistant_id": str(assistant["assistant_id"]),
        },
    }

    try:
        if payload.input is not None:
            agent.get_input_schema(config).validate(payload.input)
    except ValidationError as e:
        raise RequestValidationError(e.errors(), body=payload)

    print(assistant["config"]["configurable"]["type==agent/tools"])
    # Start CodeInterpreter
    for t in assistant["config"]["configurable"]["type==agent/tools"]:
        if t["type"] == AvailableTools.CODE_INTERPRETER:
            await get_code_interpreter().astart()

    return payload.input, config


@router.post("")
async def create_run(
    payload: CreateRunPayload,
    user: AuthedUser,
    background_tasks: BackgroundTasks,
):
    """Create a run."""
    input_, config = await _run_input_and_config(payload, user["user_id"])
    background_tasks.add_task(agent.ainvoke, input_, config)
    return {"status": "ok"}  # TODO add a run id


@router.post("/stream")
async def stream_run(
    payload: CreateRunPayload,
    user: AuthedUser,
):
    """Create a run."""
    input_, config = await _run_input_and_config(payload, user["user_id"])

    return EventSourceResponse(to_sse(astream_state(agent, input_, config)))


@router.get("/input_schema")
async def input_schema() -> dict:
    """Return the input schema of the runnable."""
    return agent.get_input_schema().schema()


@router.get("/output_schema")
async def output_schema() -> dict:
    """Return the output schema of the runnable."""
    return agent.get_output_schema().schema()


@router.get("/config_schema")
async def config_schema() -> dict:
    """Return the config schema of the runnable."""
    return agent.config_schema().schema()


if tracing_is_enabled():
    langsmith_client = langsmith.client.Client()

    @router.post("/feedback")
    def create_run_feedback(feedback_create_req: FeedbackCreateRequest) -> dict:
        """
        Send feedback on an individual run to langsmith

        Note that a successful response means that feedback was successfully
        submitted. It does not guarantee that the feedback is recorded by
        langsmith. Requests may be silently rejected if they are
        unauthenticated or invalid by the server.
        """

        langsmith_client.create_feedback(
            feedback_create_req.run_id,
            feedback_create_req.key,
            score=feedback_create_req.score,
            value=feedback_create_req.value,
            comment=feedback_create_req.comment,
            source_info={
                "from_langserve": True,
            },
        )

        return {"status": "ok"}
