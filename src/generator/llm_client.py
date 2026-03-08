import json
import time
from http.client import RemoteDisconnected
from typing import Optional
from urllib import error, request

from src.errors import UpstreamModelError


def call_chat_completion(
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_content: str,
    temperature: float,
    max_tokens: int,
    request_timeout: int,
    max_retries: int = 3,
) -> str:
    endpoint = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
    }
    data = json.dumps(payload).encode("utf-8")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        req = request.Request(endpoint, data=data, headers=headers, method="POST")
        try:
            with request.urlopen(req, timeout=request_timeout) as resp:
                response_data = json.loads(resp.read().decode("utf-8"))
            break
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from model endpoint: {body}") from exc
        except (error.URLError, RemoteDisconnected) as exc:
            last_error = exc
            if attempt < max_retries - 1:
                time.sleep(1 + attempt)
                continue
            raise UpstreamModelError(f"Could not reach model endpoint: {exc}") from exc
    else:
        raise UpstreamModelError(f"Could not reach model endpoint: {last_error}")

    try:
        return response_data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise UpstreamModelError(f"Unexpected response format: {response_data}") from exc

