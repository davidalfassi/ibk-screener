from __future__ import annotations

import asyncio
import logging

from ib_async import IB

from src.config.loader import IBGatewayConfig

log = logging.getLogger(__name__)


class IBClient:
    """Async context manager that owns the IB connection lifecycle."""

    def __init__(self, config: IBGatewayConfig) -> None:
        self._config = config
        self._ib = IB()

    async def __aenter__(self) -> IB:
        await self._connect()
        return self._ib

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._ib.isConnected():
            self._ib.disconnect()
            log.info("Disconnected from IB Gateway")

    async def _connect(self, attempt: int = 0) -> None:
        cfg = self._config
        client_id = cfg.client_id + attempt  # increment on retry to avoid stale-session clash

        log.info(
            "Connecting to IB Gateway at %s:%d (clientId=%d)...",
            cfg.host, cfg.port, client_id,
        )
        try:
            await asyncio.wait_for(
                self._ib.connectAsync(cfg.host, cfg.port, clientId=client_id),
                timeout=cfg.timeout,
            )
            log.info("Connected to IB Gateway")
        except ConnectionRefusedError:
            raise ConnectionRefusedError(
                f"IB Gateway refused connection on {cfg.host}:{cfg.port}. "
                "Make sure IB Gateway (or TWS) is running and API connections are enabled."
            )
        except asyncio.TimeoutError:
            if attempt == 0:
                # First timeout — likely stale clientId, retry with next id
                log.warning("Connection timeout on clientId %d, retrying with %d after 5s", client_id, client_id + 1)
                await asyncio.sleep(5)
                await self._connect(attempt=1)
            else:
                raise TimeoutError(
                    f"Connection to IB Gateway timed out after {cfg.timeout}s. "
                    "Check host/port in config/settings.yaml."
                )
        except Exception as e:
            # Duplicate clientId after unclean disconnect — retry once with next id
            if attempt == 0 and "already connected" in str(e).lower():
                log.warning("clientId %d already in use, retrying with %d after 5s", client_id, client_id + 1)
                await asyncio.sleep(5)
                await self._connect(attempt=1)
            else:
                raise
