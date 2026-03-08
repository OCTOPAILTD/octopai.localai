from pydantic import BaseModel, Field


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

