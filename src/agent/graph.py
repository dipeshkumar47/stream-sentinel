"""LangGraph ReAct agent wiring."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from langchain_ollama import ChatOllama
from langgraph.prebuilt import create_react_agent

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.prompts import SYSTEM_PROMPT
from agent.tools import log_stats, vector_search
from config import OLLAMA_BASE_URL, OLLAMA_MODEL

llm = ChatOllama(
    model=OLLAMA_MODEL,
    base_url=OLLAMA_BASE_URL,
    temperature=0,
)


async def build_agent_graph(extra_tools: list[Any] | None = None):
    """Build and return a compiled LangGraph ReAct agent."""
    tools = [vector_search, log_stats] + (extra_tools or [])
    return create_react_agent(
        model=llm,
        tools=tools,
        prompt=SYSTEM_PROMPT,
        name="sre-analyst",
    )