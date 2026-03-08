"""Application configuration settings."""
from functools import lru_cache
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # # App Settings
    # APP_NAME: str = "AI Data Quality Agent"
    # APP_VERSION: str = "1.0.0"
    # DEBUG: bool = Field(default=False, env="DEBUG")
    # App Settings
    APP_NAME: str = "AI Data Quality Agent"
    APP_VERSION: str = "1.0.2"  # ← bumped
    DEBUG: bool = Field(default=False, env="DEBUG")
    
    # API Settings
    API_V1_PREFIX: str = "/api/v1"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # Security
    SECRET_KEY: str = Field(default="your-secret-key-change-in-production", env="SECRET_KEY")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    ALGORITHM: str = "HS256"
    
    # Database
    DATABASE_URL: str = Field(default="postgresql://postgres:postgres@localhost:5432/dataquality", env="DATABASE_URL")
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    
    # LLM Settings - Support for Ollama, LM Studio, and cloud providers
    LLM_PROVIDER: str = Field(default="ollama", env="LLM_PROVIDER")  # ollama, lmstudio, openai, anthropic
    
    # Ollama Settings
    OLLAMA_BASE_URL: str = Field(default="http://localhost:11434", env="OLLAMA_BASE_URL")
    OLLAMA_MODEL: str = Field(default="qwen3.5:397b-cloud", env="OLLAMA_MODEL")
    
    # LM Studio Settings
    LMSTUDIO_BASE_URL: str = Field(default="http://localhost:1234/v1", env="LMSTUDIO_BASE_URL")
    LMSTUDIO_MODEL: str = Field(default="local-model", env="LMSTUDIO_MODEL")
    LMSTUDIO_API_KEY: str = Field(default="lm-studio", env="LMSTUDIO_API_KEY")
    
    # OpenAI Settings
    OPENAI_API_KEY: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    OPENAI_MODEL: str = Field(default="gpt-4", env="OPENAI_MODEL")
    
    # Anthropic Settings
    ANTHROPIC_API_KEY: Optional[str] = Field(default=None, env="ANTHROPIC_API_KEY")
    ANTHROPIC_MODEL: str = Field(default="claude-3-5-sonnet-20241022", env="ANTHROPIC_MODEL")
    
    # Google Gemini Settings
    GEMINI_API_KEY: Optional[str] = Field(default=None, env="GEMINI_API_KEY")
    GEMINI_MODEL: str = Field(default="gemini-2.5-pro", env="GEMINI_MODEL")
    GEMINI_RPM_LIMIT: int = Field(default=15, env="GEMINI_RPM_LIMIT")  # Requests per minute
    GEMINI_TPM_LIMIT: int = Field(default=250000, env="GEMINI_TPM_LIMIT")  # Tokens per minute
    GEMINI_RPD_LIMIT: int = Field(default=2000, env="GEMINI_RPD_LIMIT")  # Requests per day
    GEMINI_COOLDOWN_SEC: float = Field(default=4.0, env="GEMINI_COOLDOWN_SEC")  # Gap between requests
    
    # Groq Settings
    # Use a comma-separated list of keys for round-robin rotation
    GROQ_API_KEYS: Optional[str] = Field(default=None, env="GROQ_API_KEYS")
    GROQ_MODEL: str = Field(default="llama-3.2-90b-vision-preview", env="GROQ_MODEL")
    
    # OpenRouter Settings
    # Use a comma-separated list of keys for round-robin rotation
    OPENROUTER_API_KEYS: Optional[str] = Field(default=None, env="OPENROUTER_API_KEYS")
    OPENROUTER_MODEL: str = Field(default="deepseek/deepseek-r1", env="OPENROUTER_MODEL")
    OPENROUTER_BASE_URL: str = Field(default="https://openrouter.ai/api/v1", env="OPENROUTER_BASE_URL")
    
    # Embedding Settings
    EMBEDDING_PROVIDER: str = Field(default="ollama", env="EMBEDDING_PROVIDER")
    EMBEDDING_MODEL: str = Field(default="nomic-embed-text", env="EMBEDDING_MODEL")
    
    # Vector Database
    VECTOR_DB_PATH: str = Field(default="./chroma_db", env="VECTOR_DB_PATH")
    
    # # Validation Settings
    # MAX_SAMPLE_SIZE: int = 10000
    # DEFAULT_SAMPLE_SIZE: int = 1000
    # MAX_CONTEXT_TOKENS: int = 128000

    # Validation Settings ← NEW FULL-SCAN SECTION
    MAX_SAMPLE_SIZE: int = 10000
    DEFAULT_SAMPLE_SIZE: int = 1000
    MAX_FULL_SCAN_ROWS: int = 50000          # ← safety limit
    DEFAULT_FULL_SCAN: bool = False
    FULL_SCAN_WARNING_THRESHOLD: int = 10000 # warn user above this
    MAX_CONTEXT_TOKENS: int = 128000
    
    # Azure Settings
    AZURE_STORAGE_ACCOUNT: Optional[str] = Field(default=None, env="AZURE_STORAGE_ACCOUNT")
    AZURE_TENANT_ID: Optional[str] = Field(default=None, env="AZURE_TENANT_ID")
    AZURE_CLIENT_ID: Optional[str] = Field(default=None, env="AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET: Optional[str] = Field(default=None, env="AZURE_CLIENT_SECRET")
    
    # AWS Settings
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    AWS_REGION: str = Field(default="us-east-1", env="AWS_REGION")
    
    # GCP Settings
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = Field(default=None, env="GOOGLE_APPLICATION_CREDENTIALS")
    GOOGLE_PROJECT_ID: Optional[str] = Field(default=None, env="GOOGLE_PROJECT_ID")
    
    # Databricks Settings
    DATABRICKS_HOST: Optional[str] = Field(default=None, env="DATABRICKS_HOST")
    DATABRICKS_TOKEN: Optional[str] = Field(default=None, env="DATABRICKS_TOKEN")
    
    # CORS
    CORS_ORIGINS: List[str] = Field(default=["http://localhost:5173", "http://localhost:3000"], env="CORS_ORIGINS")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
