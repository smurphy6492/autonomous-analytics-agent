"""Base agent class — Claude API integration with retry logic and dev-time caching."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, TypeVar

import anthropic
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-sonnet-4-6"
_DEFAULT_MAX_TOKENS = 16384
_RETRY_BASE_DELAY = 1.0  # seconds; doubled on each attempt

T = TypeVar("T", bound=BaseModel)


class AgentError(Exception):
    """Raised when all retry attempts are exhausted or a non-retryable error occurs."""


class BaseAgent:
    """Base class for all analytics pipeline agents.

    Provides:
    - Claude API calls with structured JSON output (via system-prompt instruction).
    - 3-attempt retry with exponential backoff on transient errors.
    - Optional response caching to ``.cache/`` to avoid redundant API calls
      during development.  Set ``cache_dir=None`` to disable.

    Subclasses implement ``system_prompt`` (a property or attribute) and call
    ``self.call(...)`` or ``self.call_structured(...)`` from their public methods.
    """

    def __init__(
        self,
        client: anthropic.Anthropic,
        model: str = _DEFAULT_MODEL,
        cache_dir: Path | None = Path(".cache"),
        max_retries: int = 3,
    ) -> None:
        self._client = client
        self._model = model
        self._cache_dir = cache_dir
        self._max_retries = max_retries

        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public call interface
    # ------------------------------------------------------------------

    def call(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        """Call Claude and return the raw text response.

        Args:
            system_prompt: The agent's role and instructions.
            user_prompt: The specific task for this invocation.

        Returns:
            The text content of Claude's first message block.

        Raises:
            AgentError: If all retry attempts fail.
        """
        return self._call_with_retry(system_prompt, user_prompt)

    def call_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
    ) -> T:
        """Call Claude and parse the response as a Pydantic model.

        The system prompt is augmented to instruct Claude to respond with
        valid JSON matching the model's schema.  JSON is extracted from the
        response text before parsing.

        Args:
            system_prompt: The agent's role and instructions.
            user_prompt: The specific task for this invocation.
            response_model: Pydantic model class to parse the response into.

        Returns:
            A validated instance of ``response_model``.

        Raises:
            AgentError: If all retry attempts fail or JSON parsing fails.
        """
        json_instruction = (
            "\n\nIMPORTANT: Respond with a single valid JSON object only. "
            "Do not include any explanation, markdown code fences, or text "
            "outside the JSON object. The JSON must conform to this schema:\n"
            + json.dumps(response_model.model_json_schema(), indent=2)
        )
        augmented_system = system_prompt + json_instruction

        raw = self._call_with_retry(augmented_system, user_prompt)

        try:
            data = _extract_json(raw)
            return response_model.model_validate(data)
        except (json.JSONDecodeError, ValueError) as exc:
            raise AgentError(
                f"Failed to parse response as {response_model.__name__}: {exc}\n"
                f"Raw response:\n{raw}"
            ) from exc

    # ------------------------------------------------------------------
    # Retry loop
    # ------------------------------------------------------------------

    def _call_with_retry(self, system_prompt: str, user_prompt: str) -> str:
        """Call Claude with up to ``_max_retries`` attempts.

        Retries on:
        - ``anthropic.RateLimitError`` (429)
        - ``anthropic.APIStatusError`` with status >= 500
        - ``anthropic.APIConnectionError``

        Other errors (e.g. 400 Bad Request) are not retried.
        """
        cache_key = _make_cache_key(self._model, system_prompt, user_prompt)
        cached = self._load_from_cache(cache_key)
        if cached is not None:
            logger.debug("Cache hit for key %s", cache_key[:12])
            return cached

        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.debug(
                    "API call attempt %d/%d (model=%s)",
                    attempt,
                    self._max_retries,
                    self._model,
                )
                text = self._api_call(system_prompt, user_prompt)
                self._save_to_cache(cache_key, text)
                return text

            except anthropic.RateLimitError as exc:
                logger.warning("Rate limit hit (attempt %d): %s", attempt, exc)
                last_exc = exc
            except anthropic.APIStatusError as exc:
                if exc.status_code >= 500:
                    logger.warning(
                        "Server error %d (attempt %d): %s",
                        exc.status_code,
                        attempt,
                        exc,
                    )
                    last_exc = exc
                else:
                    raise AgentError(f"Non-retryable API error: {exc}") from exc
            except anthropic.APIConnectionError as exc:
                logger.warning("Connection error (attempt %d): %s", attempt, exc)
                last_exc = exc

            if attempt < self._max_retries:
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.info("Retrying in %.1fs…", delay)
                time.sleep(delay)

        raise AgentError(
            f"All {self._max_retries} attempts failed. Last error: {last_exc}"
        ) from last_exc

    def _api_call(self, system_prompt: str, user_prompt: str) -> str:
        """Single (non-retried) call to the Claude Messages API."""
        response = self._client.messages.create(
            model=self._model,
            max_tokens=_DEFAULT_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        block = response.content[0]
        if block.type != "text":
            raise AgentError(f"Unexpected response block type: {block.type}")
        return block.text

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_path(self, key: str) -> Path | None:
        if self._cache_dir is None:
            return None
        return self._cache_dir / f"{key}.json"

    def _load_from_cache(self, key: str) -> str | None:
        path = self._cache_path(key)
        if path is None or not path.exists():
            return None
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return str(data["response"])
        except (json.JSONDecodeError, KeyError):
            return None

    def _save_to_cache(self, key: str, response: str) -> None:
        path = self._cache_path(key)
        if path is None:
            return
        payload = {"response": response}
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _make_cache_key(model: str, system_prompt: str, user_prompt: str) -> str:
    """Return a deterministic hex digest for the given call parameters."""
    content = json.dumps(
        {"model": model, "system": system_prompt, "user": user_prompt},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(content.encode()).hexdigest()


def _extract_json(text: str) -> Any:
    """Extract a JSON object or array from *text*.

    Handles:
    - Plain JSON (the whole string is a JSON value).
    - JSON wrapped in markdown code fences (```json ... ``` or ``` ... ```).
    """
    stripped = text.strip()

    # Try the whole string first (most common when following instructions).
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Strip markdown fences and try again.
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        # Drop first line (```json or ```) and last line (```)
        inner = "\n".join(lines[1:-1]).strip()
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            pass

    # Last resort: find the first '{' and last '}' and parse the slice.
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            pass

    raise json.JSONDecodeError("No valid JSON found in response", text, 0)
