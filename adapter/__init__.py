import shared.compat_bootstrap as _cb  # noqa: F401
import sys as _sys

def _ensure_attr_chain(root_alias: str) -> None:
    root_mod = _sys.modules.get(root_alias)
    if root_mod is None:
        return
    prefix = root_alias + "."
    for k, v in list(_sys.modules.items()):
        if not k.startswith(prefix) or v is None:
            continue
        sub = k[len(prefix):]
        if "." in sub:
            continue
        if not hasattr(root_mod, sub):
            try:
                setattr(root_mod, sub, v)
            except Exception:
                pass

_ensure_attr_chain("adapter")
_ensure_attr_chain("agent")
_ensure_attr_chain("tools")
_ensure_attr_chain("config")

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
