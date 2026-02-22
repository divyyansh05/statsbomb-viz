"""FastAPI application entry point."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from api.routers import competitions, matches, charts

app = FastAPI(
    title="StatsBomb Viz API",
    description="Post-match football analytics dashboard — FIFA World Cup 2022",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(competitions.router, prefix="/api/v1", tags=["competitions"])
app.include_router(matches.router,      prefix="/api/v1", tags=["matches"])
app.include_router(charts.router,       prefix="/api/v1", tags=["charts"])

# Serve frontend — static assets + index.html at root
_frontend = Path(__file__).parent.parent / "frontend"
if _frontend.exists():
    app.mount("/static", StaticFiles(directory=str(_frontend / "static")), name="static")

    @app.get("/", tags=["frontend"])
    def serve_index():
        return FileResponse(str(_frontend / "index.html"))

else:
    @app.get("/", tags=["health"])
    def root():
        return {"status": "ok", "message": "StatsBomb Viz API is running"}


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}
