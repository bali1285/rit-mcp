"""RIT MCP server.

One MCP server for Ritchies data. Each data source lives in `sources/` and
adds its own tools (prefixed by source, e.g. `at_`). Today that's Auckland
Transport realtime; the data warehouse can slot in later the same way.

Runs on Azure Functions (Flex Consumption) as a self-hosted MCP custom
handler, and locally with `python server.py`.

    /mcp     MCP endpoint (streamable HTTP, stateless, JSON responses)
    /health  plain health check, no auth

Auth: every request except /health must send `Authorization: Bearer
<APP_API_TOKEN>`. Set REQUIRE_TOKEN=false only if something in front (such as
Functions keys or Entra sign-in) does the auth instead.

Local:
    az login
    python server.py --describe        # check the AT feed and GTFS load
    APP_API_TOKEN=<32+ chars> python server.py
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import sys

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from sources import SOURCES, at_realtime

PORT = int(os.getenv("FUNCTIONS_CUSTOMHANDLER_PORT") or os.getenv("PORT", "8000"))
APP_API_TOKEN = os.getenv("APP_API_TOKEN", "")
REQUIRE_TOKEN = os.getenv("REQUIRE_TOKEN", "true").lower() != "false"
PUBLIC_ORIGIN = os.getenv("PUBLIC_ORIGIN", "")  # optional exact https origin for browser clients

mcp = MCPServer(
    name="rit-mcp",
    instructions=(
        "Read-only data tools for Ritchies Transport. Tool names start with their "
        "source.\n\n" + "\n\n".join(s.INSTRUCTIONS for s in SOURCES)
    ),
)
for source in SOURCES:
    source.register(mcp)


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


class TokenGuard:
    """Bearer token check in front of the MCP app.

    Fails closed: if REQUIRE_TOKEN is on and APP_API_TOKEN is missing or
    shorter than 32 characters, every route except /health returns 503.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/health" or not REQUIRE_TOKEN:
            return await self.app(scope, receive, send)
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        if len(APP_API_TOKEN) < 32:
            return await self._reply(send, 503, "Server token not configured (APP_API_TOKEN, 32+ chars).")
        origin = headers.get("origin")
        if origin is not None and origin != PUBLIC_ORIGIN:
            return await self._reply(send, 403, "Origin not allowed.")
        if not hmac.compare_digest(headers.get("authorization", ""), f"Bearer {APP_API_TOKEN}"):
            return await self._reply(send, 401, "Missing or wrong bearer token.")
        return await self.app(scope, receive, send)

    @staticmethod
    async def _reply(send, status: int, message: str):
        body = json.dumps({"error": message}).encode()
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


# DNS rebinding protection is off because the app sits behind the Functions
# front end, whose host name isn't known here; the token and Origin checks
# above are the gate instead.
app = TokenGuard(
    mcp.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
        host="0.0.0.0",
    )
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--describe", action="store_true", help="print the AT feed and GTFS summary and exit")
    args = parser.parse_args()
    if args.describe:
        print(json.dumps(at_realtime.describe_feed(), indent=2, default=str))
        return
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    sys.exit(main())
