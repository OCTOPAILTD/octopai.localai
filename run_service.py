#!/usr/bin/env python3
import os

import uvicorn


def main() -> int:
    host = os.getenv("PARSER_SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("PARSER_SERVICE_PORT", "8080"))
    uvicorn.run("src.service.app:app", host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

