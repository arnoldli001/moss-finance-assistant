# -*- coding: utf-8 -*-
"""adapter 包：协议适配与归一化层（§3）。"""
from .stream_adapters import (
    NormalizedChunk,
    ThinkTagSplitter,
    CitationDocument,
    build_citation_context,
    normalize_citation_markers,
    extract_citations_from_delta,
    assign_citations_by_overlap,
    patch_system_prompt_require_reasoning_and_citations,
    create_stream_pipeline,
    build_focused_snippet,
)
from .ollama_client import (
    ollama_chat_stream,
    ollama_synthesize,
    ollama_probe,
    SynthesisResult,
)

__all__ = [
    "NormalizedChunk",
    "ThinkTagSplitter",
    "CitationDocument",
    "build_citation_context",
    "normalize_citation_markers",
    "extract_citations_from_delta",
    "assign_citations_by_overlap",
    "patch_system_prompt_require_reasoning_and_citations",
    "create_stream_pipeline",
    "build_focused_snippet",
    "ollama_chat_stream",
    "ollama_synthesize",
    "ollama_probe",
    "SynthesisResult",
]
