"""Run the behaviour worker:  python -m app.worker"""
import asyncio
import logging

from app.common.redis_client import close_redis, create_redis
from app.config import load_settings
from app.policy.params import load_policy_params
from app.worker.consumer import BehaviourWorker
from app.worker.params import load_params


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = load_settings()
    # Blocking reads must outlive the socket timeout used by the gateway.
    client = create_redis(settings)
    client.connection_pool.connection_kwargs["socket_timeout"] = 5.0
    worker = BehaviourWorker(client, load_params(), load_policy_params())
    try:
        await worker.run()
    finally:
        await close_redis(client)


if __name__ == "__main__":
    asyncio.run(main())
