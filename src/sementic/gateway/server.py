from __future__ import annotations

import uvicorn

from sementic.gateway.config import GatewaySettings


def main() -> None:
    settings = GatewaySettings()
    uvicorn.run(
        "sementic.gateway.api:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
