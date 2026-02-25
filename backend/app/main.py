"""Main FastAPI application."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.core.config import get_settings
from app.api.routes import router as api_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting up AI Data Quality Agent...")
    logger.info(f"LLM Provider: {settings.LLM_PROVIDER}")
    logger.info(f"API running on {settings.HOST}:{settings.PORT}")
    
    # Initialize services
    try:
        from app.agents.rag_service import get_rag_service
        rag_service = await get_rag_service()
        logger.info("RAG service initialized")
    except Exception as e:
        logger.warning(f"RAG service initialization failed: {str(e)}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down AI Data Quality Agent...")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="Enterprise-Grade AI-Powered Data Quality Assurance Platform",
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix=settings.API_V1_PREFIX)

# Static files for reports
import os
reports_dir = os.path.join(os.path.dirname(__file__), "reports")
os.makedirs(reports_dir, exist_ok=True)
app.mount("/reports", StaticFiles(directory=reports_dir), name="reports")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "api": settings.API_V1_PREFIX,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
