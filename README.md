# stream-sentinel 🛡️

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-teal.svg)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-ReAct-purple.svg)](https://github.com/langchain-ai/langgraph)
[![ChromaDB](https://img.shields.io/badge/ChromaDB-0.6.3-orange.svg)](https://www.trychroma.com/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-red.svg)](https://kafka.apache.org/)
[![MCP](https://img.shields.io/badge/MCP-FastMCP-purple.svg)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> Real-time log streaming, semantic vector search, and AI-powered SRE incident analysis.

Built with **Apache Kafka**, **ChromaDB**, **LangGraph ReAct Agent**, **Ollama (llama3.2:3b)**, and **FastAPI**.

## Overview

`stream-sentinel` ingests live microservice logs through Kafka, vectorizes them in real-time using
HuggingFace sentence embeddings, and stores them in ChromaDB. A LangGraph ReAct agent answers
natural-language questions about system health — grounded in live log data, not model memory.

## Stack

| Component | Role |
|---|---|
| Apache Kafka (KRaft) | High-throughput event streaming |
| ChromaDB 0.6.3 | Vector database for semantic search |
| all-MiniLM-L6-v2 | Local 384-dim sentence embeddings |
| LangGraph + Ollama | Autonomous ReAct reasoning agent |
| FastAPI + Uvicorn | REST and SSE API gateway |
| FastMCP | Model Context Protocol server |

> See [docs/technical-guide.md](docs/technical-guide.md) for full architecture.