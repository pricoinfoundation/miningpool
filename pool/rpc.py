"""JSON-RPC 1.0 client for pricoind. Cookie- and rpcauth-aware.

Synchronous on purpose: stratum hot path doesn't call this directly;
jobs.py polls in its own thread / asyncio task at template-refresh
intervals and on block-found events.
"""
from __future__ import annotations

import base64
import http.client
import json
import os
import pathlib
from typing import Any


class RPCError(RuntimeError):
    def __init__(self, code: int, message: str):
        super().__init__(f"RPC {code}: {message}")
        self.code = code
        self.message = message


class PricoinRPC:
    """One client per pricoind. Keeps a short-lived HTTP connection per
    call — plenty for the pool's call rate (a few per second peak).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8332,
                 user: str | None = None, password: str | None = None,
                 datadir: str | None = None, wallet: str | None = None,
                 timeout: float = 30.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.wallet = wallet
        if user and password is not None:
            self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        else:
            self._auth = self._read_cookie(datadir or "~/.pricoin")

    @staticmethod
    def _read_cookie(datadir: str) -> str:
        path = pathlib.Path(os.path.expanduser(datadir)) / ".cookie"
        if not path.exists():
            raise RPCError(-1, f"no rpcuser/rpcpassword and no cookie at {path}")
        return "Basic " + base64.b64encode(path.read_bytes()).decode()

    def call(self, method: str, *params: Any) -> Any:
        endpoint = f"/wallet/{self.wallet}" if self.wallet else "/"
        body = json.dumps({"jsonrpc": "1.0", "id": "pool", "method": method, "params": list(params)})
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            conn.request("POST", endpoint, body, {
                "Content-Type": "application/json",
                "Authorization": self._auth,
            })
            resp = conn.getresponse()
            data = json.loads(resp.read())
        finally:
            conn.close()
        err = data.get("error")
        if err:
            raise RPCError(err.get("code", -1), err.get("message", "unknown"))
        return data["result"]
