from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="LLM Station", version="0.1.0", lifespan=lifespan)
    return app


app = create_app()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
