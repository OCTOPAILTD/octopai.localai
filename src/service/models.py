from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# OpenAI-compatible chat completion models (used by /v1/chat/completions)
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str = "local-python-sql"
    messages: list[ChatMessage]
    stream: bool = False
    temperature: float = 0.0
    max_tokens: int = 1536
    top_p: float = 1.0


class ChatCompletionChunkDelta(BaseModel):
    role: str | None = None
    content: str | None = None


class ChatCompletionChunkChoice(BaseModel):
    index: int = 0
    delta: ChatCompletionChunkDelta
    finish_reason: str | None = None


class ChatCompletionChunk(BaseModel):
    id: str
    object: str = "chat.completion.chunk"
    created: int
    model: str
    choices: list[ChatCompletionChunkChoice]


# ---------------------------------------------------------------------------
# Existing parse models
# ---------------------------------------------------------------------------

class ParseRequest(BaseModel):
    python_code: str | None = Field(default=None)
    file_path: str | None = Field(default=None)
    file_name: str = Field(default="input.py")
    prompt_file: str = Field(default="prompt_short.txt")
    output_dir: str = Field(default="sql_outputs_service")
    report_dir: str = Field(default="reports_service")
    max_tokens: int = Field(default=1536)
    strict_validation: bool = Field(default=False)
    dialect: str = Field(default="tsql")


class ParseResponse(BaseModel):
    sql: str
    report_path: str
    output_path: str


class BatchParseRequest(BaseModel):
    requests: list[ParseRequest]


class BatchParseResponse(BaseModel):
    results: list[ParseResponse]

