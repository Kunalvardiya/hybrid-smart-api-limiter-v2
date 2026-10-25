"""No-Docker development stack: gateway + backend + worker in ONE process, on an in-memory
Redis stand-in (fakeredis) that is also reachable over TCP so tools can inspect it.

    python -m tools.dev_stack            # Ctrl+C stops everything

NOT the real deployment. fakeredis is Python, single-process, has no persistence and no password,
and runs gateway, backend and worker in one event loop, so latency and throughput numbers from this
stack mean nothing. Use it to try the system and the Locust scenarios when Docker is unavailable.
The real stack is `docker compose up`. (A multi-process variant over fakeredis's TCP server was tried and
rejected: it dropped connections under concurrent Lua scripts, which made the limiter fail open.)

Listens on 127.0.0.1 only. Gateway :8000 (backend is mounted in-process), Redis stand-in :6390
(connect with redis://127.0.0.1:6390/0, e.g. load/observe.py --redis-url ...).
Settings come from the usual environment variables (BUCKET_*, SCORING_*, POLICY_* ...).
"""
import asyncio
import logging
import os
import threading

import fakeredis
import httpx
import uvicorn
from fakeredis import TcpFakeServer

from app.backend.main import app as backend_app
from app.gateway import main as gateway_main
from app.policy.params import load_policy_params
from app.worker.consumer import BehaviourWorker
from app.worker.params import load_params

REDIS_PORT = int(os.environ.get("DEV_REDIS_PORT", "6390"))
GATEWAY_PORT = int(os.environ.get("DEV_GATEWAY_PORT", "8000"))


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)     # one INFO line per proxied request otherwise
    os.environ.setdefault("ADMIN_TOKEN", "dev-admin")        # dashboard token for local experiments only
    shared = fakeredis.FakeServer()

    tcp = TcpFakeServer(("127.0.0.1", REDIS_PORT))
    tcp.fake_server = shared                      # inspectors read the very same data
    threading.Thread(target=tcp.serve_forever, daemon=True).start()

    def make_redis(*_):
        return fakeredis.FakeAsyncRedis(server=shared, decode_responses=True)
    gateway_main.create_redis = make_redis
    gateway_main.create_http_client = lambda _s: httpx.AsyncClient(
        transport=httpx.ASGITransport(app=backend_app), base_url="http://backend")

    worker = BehaviourWorker(make_redis(), load_params(), load_policy_params())

    async def run_worker() -> None:
        # fakeredis' blocking XREADGROUP would stall the shared event loop, so poll instead.
        await worker.setup()
        logging.getLogger("worker").info("behaviour worker started (polling)")
        while True:
            if not await worker.process_once(block_ms=None):
                await asyncio.sleep(0.2)
    worker_task = asyncio.create_task(run_worker())

    config = uvicorn.Config(gateway_main.app, host="127.0.0.1", port=GATEWAY_PORT, log_level="warning")
    print(f"gateway   http://127.0.0.1:{GATEWAY_PORT}   (backend mounted in-process)\n"
          f"redis     redis://127.0.0.1:{REDIS_PORT}/0   (fakeredis stand-in, no password)\n"
          f"dashboard http://127.0.0.1:{GATEWAY_PORT}/dashboard   token: {os.environ['ADMIN_TOKEN']}\n"
          f"Ctrl+C to stop.", flush=True)
    try:
        await uvicorn.Server(config).serve()
    finally:
        worker.stop()
        worker_task.cancel()
        tcp.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
