"""Demo protected API that sits behind the gateway.

The /api/fail/{code} endpoint lets tests and Locust produce 4xx/5xx
responses on demand, which feeds the failure-rate feature (F) later.
"""
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Protected Demo API")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/ping")
async def ping():
    return {"message": "pong"}


@app.get("/api/items/{item_id}")
async def get_item(item_id: int):
    return {"id": item_id, "name": f"item-{item_id}"}


@app.get("/api/users/{user_id}")
async def get_user(user_id: int):
    return {"id": user_id, "name": f"user-{user_id}"}


@app.get("/api/orders")
async def list_orders():
    return {"orders": []}


@app.get("/api/search")
async def search(q: str = ""):
    return {"query": q, "results": []}


@app.get("/api/fail/{code}")
async def fail(code: int):
    if not 400 <= code <= 599:
        raise HTTPException(status_code=400, detail="code must be 400-599")
    raise HTTPException(status_code=code, detail="intentional failure")
