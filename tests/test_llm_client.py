"""Tests for the provider-neutral LLM adapter (sql_logic_extractor.llm_client).

Covers provider resolution (explicit arg -> SLE_LLM_PROVIDER env ->
auto-detect from credentials -> clear error) and Azure client construction
against a *fake* openai module, so nothing here needs a real SDK or network.

Run: python3 -m pytest tests/test_llm_client.py -v
"""

import sys
import types

import pytest

from sql_logic_extractor.llm_client import (
    _resolve_provider,
    make_llm_client,
    OpenAIClient,
)


# All env vars that influence provider resolution -- cleared before each test
# so a developer's real shell config can't leak in.
_ENV_KEYS = [
    "SLE_LLM_PROVIDER",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_DEPLOYMENT",
    "AZURE_OPENAI_API_VERSION",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "GEMINI_API_KEY",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in _ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_resolve_explicit_arg_wins(monkeypatch):
    monkeypatch.setenv("SLE_LLM_PROVIDER", "gemini")
    # Explicit arg overrides the env var.
    assert _resolve_provider("openai") == "openai"


def test_resolve_from_env(monkeypatch):
    monkeypatch.setenv("SLE_LLM_PROVIDER", "Azure-OpenAI")  # case-insensitive
    assert _resolve_provider(None) == "azure-openai"


def test_resolve_autodetect_azure(monkeypatch):
    # Only Azure creds present -> auto-detect picks azure-openai.
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com/")
    assert _resolve_provider(None) == "azure-openai"


def test_resolve_autodetect_openai(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    assert _resolve_provider(None) == "openai"


def test_resolve_raises_when_unconfigured():
    with pytest.raises(ValueError) as excinfo:
        _resolve_provider(None)
    # Error message should name the providers so the user knows their options.
    msg = str(excinfo.value)
    assert "azure-openai" in msg and "OPENAI_API_KEY" in msg


def _install_fake_openai(monkeypatch):
    """Install a fake `openai` module exposing AzureOpenAI / OpenAI so the
    adapter can be constructed without the real SDK."""
    fake = types.ModuleType("openai")

    class _FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake.AzureOpenAI = _FakeClient
    fake.OpenAI = _FakeClient
    monkeypatch.setitem(sys.modules, "openai", fake)
    return _FakeClient


def test_make_llm_client_azure_uses_deployment_as_model(monkeypatch):
    _install_fake_openai(monkeypatch)
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com/")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "key-123")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-prod")

    client = make_llm_client(provider="azure-openai")
    assert isinstance(client, OpenAIClient)
    # On Azure, `model` is the deployment name, not a public model id.
    assert client._model == "gpt-4o-prod"
    # The Azure endpoint/key/version were threaded into the SDK client.
    assert client._client.kwargs["azure_endpoint"] == "https://x.openai.azure.com/"
    assert client._client.kwargs["api_key"] == "key-123"
    assert "api_version" in client._client.kwargs


def test_make_llm_client_azure_missing_deployment_raises(monkeypatch):
    _install_fake_openai(monkeypatch)
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com/")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "key-123")
    # No AZURE_OPENAI_DEPLOYMENT -> clear error.
    with pytest.raises(ValueError) as excinfo:
        make_llm_client(provider="azure-openai")
    assert "deployment" in str(excinfo.value).lower()


def test_complete_json_roundtrip_with_fake_openai(monkeypatch):
    """End-to-end: complete_json builds a chat completion request and parses
    the JSON content back -- using a fake SDK, no network."""
    fake = types.ModuleType("openai")

    class _FakeResp:
        def __init__(self, content):
            self.choices = [types.SimpleNamespace(
                message=types.SimpleNamespace(content=content))]

    class _FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(create=self._create))
            self.last_request = None

        def _create(self, **kwargs):
            self.last_request = kwargs
            return _FakeResp('{"english_definition": "ok"}')

    fake.AzureOpenAI = _FakeClient
    fake.OpenAI = _FakeClient
    monkeypatch.setitem(sys.modules, "openai", fake)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    client = make_llm_client(provider="openai")
    result = client.complete_json("SYS", "USER")
    assert result == {"english_definition": "ok"}
    # JSON mode requested, and both prompts forwarded as messages.
    req = client._client.last_request
    assert req["response_format"] == {"type": "json_object"}
    roles = [m["role"] for m in req["messages"]]
    assert roles == ["system", "user"]
