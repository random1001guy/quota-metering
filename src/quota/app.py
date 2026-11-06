"""HTTP API of the quota service.

Endpoints (proposed shape; handlers are filled in as the store code lands):
  POST /v1/reserve   {org_id, feature, units, request_id} -> hold units, all-or-nothing
  POST /v1/commit    {org_id, request_id}                 -> reserved units become used
  POST /v1/release   {org_id, request_id}                 -> reserved units go back
  GET  /v1/usage/{org_id}/{feature}                       -> used, reserved, limit, resets_at
"""
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Quota service")


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.post("/v1/reserve")
async def reserve() -> dict:
    raise HTTPException(501, "not implemented")


@app.post("/v1/commit")
async def commit() -> dict:
    raise HTTPException(501, "not implemented")


@app.post("/v1/release")
async def release() -> dict:
    raise HTTPException(501, "not implemented")


@app.get("/v1/usage/{org_id}/{feature}")
async def usage(org_id: int, feature: str) -> dict:
    raise HTTPException(501, "not implemented")
