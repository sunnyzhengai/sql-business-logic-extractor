#!/usr/bin/env python3
"""Provider-neutral LLM client adapters for Tool 3 / Tool 4 LLM mode.

The two LLM call sites in ``business_logic.py`` (per-column translation and
query-level summarisation) only need to send a system + user prompt and get a
parsed JSON object back. Everything that differs by vendor -- client
construction, model/deployment name, the JSON-mode flag, response parsing --
is hidden behind a tiny adapter that exposes a single method:

    complete_json(system_prompt, user_prompt, *, temperature=0.3) -> dict

Healthcare-safe guarantee: every vendor SDK import is **lazy** (inside the
adapter constructor / method), so an engineered-mode-only install never pulls
``openai`` or ``google.genai`` into ``sys.modules`` -- auditable for hospital
procurement. Customers "bring their own key" (BYOK) via environment variables.

Supported providers (``SLE_LLM_PROVIDER`` / ``make_llm_client(provider=...)``):
  - ``"azure-openai"`` -- Azure OpenAI deployment (the primary target).
  - ``"openai"``       -- direct OpenAI API.
  - ``"gemini"``       -- Google Gemini (the original scaffolded provider).
"""

from __future__ import annotations

import json
import os
from typing import Optional


# Default Azure API version used when AZURE_OPENAI_API_VERSION is unset. Pinned
# to a GA version that supports JSON response_format on chat completions.
_DEFAULT_AZURE_API_VERSION = "2024-10-21"
# Default model for direct OpenAI when OPENAI_MODEL is unset.
_DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
# Default model for Gemini (matches the original scaffold).
_DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"


class OpenAIClient:
    """Adapter for OpenAI-family chat completions -- covers both Azure OpenAI
    and the direct OpenAI API. The two differ only in how the underlying
    client is constructed and in what ``model`` means (Azure: a *deployment*
    name; OpenAI: a model id); the request/response shape is identical."""

    def __init__(self, *, azure: bool, api_key: Optional[str] = None,
                 model: Optional[str] = None):
        # Lazy import: only loaded when LLM mode is actually used.
        if azure:
            from openai import AzureOpenAI

            endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
            api_key = api_key or os.environ.get("AZURE_OPENAI_API_KEY")
            if not endpoint or not api_key:
                raise ValueError(
                    "Azure OpenAI requires AZURE_OPENAI_ENDPOINT and "
                    "AZURE_OPENAI_API_KEY in the environment (BYOK)."
                )
            api_version = os.environ.get("AZURE_OPENAI_API_VERSION",
                                         _DEFAULT_AZURE_API_VERSION)
            # On Azure, `model` is the *deployment* name, not a model id.
            self._model = model or os.environ.get("AZURE_OPENAI_DEPLOYMENT")
            if not self._model:
                raise ValueError(
                    "Azure OpenAI requires a deployment name -- set "
                    "AZURE_OPENAI_DEPLOYMENT or pass model=."
                )
            self._client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key,
                                       api_version=api_version)
        else:
            from openai import OpenAI

            api_key = api_key or os.environ.get("OPENAI_API_KEY")
            if not api_key:
                raise ValueError(
                    "OpenAI requires OPENAI_API_KEY in the environment (BYOK)."
                )
            self._model = model or os.environ.get("OPENAI_MODEL",
                                                  _DEFAULT_OPENAI_MODEL)
            self._client = OpenAI(api_key=api_key)

    def complete_json(self, system_prompt: str, user_prompt: str, *,
                      temperature: float = 0.3) -> dict:
        """Send system + user prompts in JSON mode and return the parsed dict.
        Raises on transport/parse errors -- the caller owns the fallback."""
        # response_format json_object requires the word "json" somewhere in the
        # prompt; our system prompts already say "Output JSON:", so we comply.
        resp = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content)


class GeminiClient:
    """Adapter for Google Gemini -- lifts the original scaffold's call shape
    (``models.generate_content`` with a JSON response mime type)."""

    def __init__(self, *, api_key: Optional[str] = None,
                 model: Optional[str] = None):
        # Lazy import: only loaded when LLM mode is actually used.
        from google import genai

        api_key = api_key or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise ValueError(
                "Gemini requires GEMINI_API_KEY in the environment (BYOK)."
            )
        self._client = genai.Client(api_key=api_key)
        self._model = model or _DEFAULT_GEMINI_MODEL

    def complete_json(self, system_prompt: str, user_prompt: str, *,
                      temperature: float = 0.3) -> dict:
        """Send system + user prompts in JSON mode and return the parsed dict.
        Raises on transport/parse errors -- the caller owns the fallback."""
        from google.genai import types

        resp = self._client.models.generate_content(
            model=self._model,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=temperature,
                response_mime_type="application/json",
            ),
        )
        return json.loads(resp.text)


def _resolve_provider(provider: Optional[str]) -> str:
    """Resolve the active provider: explicit arg -> SLE_LLM_PROVIDER env ->
    auto-detect from which credentials are present. Raises a clear ValueError
    listing the options if nothing is configured."""
    chosen = (provider or os.environ.get("SLE_LLM_PROVIDER") or "").strip().lower()
    if chosen:
        return chosen
    # Auto-detect from available credentials.
    if os.environ.get("AZURE_OPENAI_ENDPOINT"):
        return "azure-openai"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    if os.environ.get("GEMINI_API_KEY"):
        return "gemini"
    raise ValueError(
        "No LLM provider configured. Set SLE_LLM_PROVIDER to one of "
        "'azure-openai', 'openai', 'gemini' (or provide the matching "
        "credentials: AZURE_OPENAI_ENDPOINT+AZURE_OPENAI_API_KEY, "
        "OPENAI_API_KEY, or GEMINI_API_KEY)."
    )


def make_llm_client(provider: Optional[str] = None,
                    api_key: Optional[str] = None,
                    model: Optional[str] = None):
    """Build a provider-neutral LLM client. Customers bring their own key
    (BYOK) via environment variables -- you don't pay for their LLM use unless
    you're hosting the SaaS tier. The returned object exposes ``complete_json``.

    `provider` selects the backend (defaults to SLE_LLM_PROVIDER, then
    auto-detection); `api_key`/`model` override the env-derived values."""
    chosen = _resolve_provider(provider)
    if chosen in ("azure-openai", "azure", "azure_openai"):
        return OpenAIClient(azure=True, api_key=api_key, model=model)
    if chosen == "openai":
        return OpenAIClient(azure=False, api_key=api_key, model=model)
    if chosen == "gemini":
        return GeminiClient(api_key=api_key, model=model)
    raise ValueError(
        f"Unknown LLM provider {chosen!r}. Expected one of: "
        "'azure-openai', 'openai', 'gemini'."
    )
