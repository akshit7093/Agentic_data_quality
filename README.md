# AI Data Quality Agent

An Enterprise-Grade AI-Powered Data Quality Assurance Platform built with FastAPI, LangGraph, and React.

![Architecture](docs/architecture.png)

## Features

- **Multi-Source Data Connectivity**: Connect to ADLS Gen2, Databricks, AWS S3, GCP, PostgreSQL, MySQL, and local files
- **AI-Powered Validation**: Uses LangGraph agents with LLMs (Ollama, LM Studio, OpenAI, Anthropic) for intelligent data quality checks
- **RAG-Based Context Management**: Prevents hallucinations with retrieval-augmented generation
- **Hybrid Validation Modes**: Combine custom rules with AI-generated recommendations
- **Real-time Monitoring**: Dashboard with quality scores, trends, and detailed reports
- **Enterprise Security**: OAuth2, RBAC, audit trails, and encryption

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        React Frontend                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Dashboard  │  │ Validations │  │    Data Sources         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FastAPI Backend                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              LangGraph Agent Workflow                    │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │   │
│  │  │ Connect  │→│ Profile  │→│ Validate │→│ Report   │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐    │
│  │ LLM Service │  │RAG Service  │  │ Validation Engine   │    │
│  │ (Ollama,    │  │(ChromaDB)   │  │ (Rules + AI)        │    │
│  │  OpenAI...) │  │             │  │                     │    │
│  └─────────────┘  └─────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Data Connectors                             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐  │
│  │  ADLS   │ │Databricks│ │ AWS S3 │ │PostgreSQL│ │  Local  │  │
│  │  Gen2   │ │          │ │        │ │          │ │  Files  │  │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 20+
- Docker & Docker Compose (optional)
- Ollama (for local LLM) or API keys for OpenAI/Anthropic

### Option 1: Docker Compose (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd ai-data-quality-agent

# Start all services
docker-compose up -d

# Access the application
# Frontend: http://localhost:5173
# Backend API: http://localhost:8000
# API Docs: http://localhost:8000/docs
```

### Option 2: Manual Setup

#### Backend Setup

```bash
# Create virtual environment
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your configuration

# Run the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

#### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev

# Access the application at http://localhost:5173
```

## LLM Configuration

The system supports multiple LLM providers:

### Ollama (Local - Recommended for Privacy)

1. Install Ollama: https://ollama.com
2. Pull a model: `ollama pull llama3.2`
3. Set in Settings: Provider = "Ollama", Model = "llama3.2"

### LM Studio

1. Download LM Studio: https://lmstudio.ai
2. Start the local server (default: http://localhost:1234/v1)
3. Set in Settings: Provider = "LM Studio"

### OpenAI

1. Get API key from https://platform.openai.com
2. Set in Settings: Provider = "OpenAI", enter API key

### Anthropic Claude

1. Get API key from https://console.anthropic.com
2. Set in Settings: Provider = "Anthropic", enter API key

## Data Source Configuration

### Local Files

```json
{
  "source_type": "local_file",
  "connection_config": {
    "base_path": "/path/to/data/files"
  }
}
```

### PostgreSQL

```json
{
  "source_type": "postgresql",
  "connection_config": {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "username": "user",
    "password": "pass"
  }
}
```

### AWS S3

```json
{
  "source_type": "aws_s3",
  "connection_config": {
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "AKIA...",
    "secret_access_key": "..."
  }
}
```

## Validation Rules

### Rule Types

- **Column Rules**: Null checks, type validation, range checks
- **Row Rules**: Cross-column validation, business logic
- **Table Rules**: Uniqueness, row count, referential integrity
- **Pattern Rules**: Regex matching, email/phone validation
- **Statistical Rules**: Outlier detection, distribution checks

### Example Custom Rule

```json
{
  "name": "Email Format Valid",
  "rule_type": "pattern",
  "severity": "critical",
  "target_columns": ["email"],
  "config": {
    "pattern_type": "email"
  }
}
```

## API Endpoints

### Data Sources
- `GET /api/v1/datasources` - List data sources
- `POST /api/v1/datasources` - Create data source
- `POST /api/v1/datasources/{id}/test` - Test connection

### Validations
- `POST /api/v1/validate` - Submit validation
- `GET /api/v1/validate/{id}` - Get validation status
- `GET /api/v1/validate/{id}/results` - Get results

### Rules
- `GET /api/v1/rules` - List rules
- `POST /api/v1/rules` - Create rule
- `PUT /api/v1/rules/{id}` - Update rule

### AI
- `POST /api/v1/ai/recommend-rules` - Get AI rule recommendations
- `POST /api/v1/ai/analyze` - Analyze data quality

## Environment Variables

```bash
# App Settings
DEBUG=false
SECRET_KEY=your-secret-key

# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/dbname
REDIS_URL=redis://localhost:6379/0

# LLM Configuration
LLM_PROVIDER=ollama  # ollama, lmstudio, openai, anthropic
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2

# OpenAI (if using)
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4

# Anthropic (if using)
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-3-5-sonnet-20241022

# Cloud Providers
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AZURE_STORAGE_ACCOUNT=...
GOOGLE_APPLICATION_CREDENTIALS=...
```

## Development

### Running Tests

```bash
# Backend tests
cd backend
pytest

# Frontend tests
cd frontend
npm test
```

### Code Structure

```
ai-data-quality-agent/
├── backend/
│   ├── app/
│   │   ├── agents/          # LangGraph agents
│   │   ├── api/             # FastAPI routes
│   │   ├── connectors/      # Data source connectors
│   │   ├── core/            # Config & utilities
│   │   ├── models/          # Database models
│   │   ├── validation/      # Validation engine
│   │   └── main.py          # Application entry
│   ├── requirements.txt
│   └── Dockerfile.backend
├── frontend/
│   ├── src/
│   │   ├── components/      # React components
│   │   ├── hooks/           # Custom hooks
│   │   ├── pages/           # Page components
│   │   ├── services/        # API services
│   │   ├── store/           # Zustand store
│   │   └── types/           # TypeScript types
│   ├── package.json
│   └── Dockerfile
├── docker-compose.yml
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -am 'Add new feature'`
4. Push to branch: `git push origin feature/my-feature`
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Support

For issues and questions:
- GitHub Issues: [Create an issue](https://github.com/your-org/ai-data-quality-agent/issues)
- Documentation: [Full Docs](https://docs.ai-data-quality-agent.com)

## Roadmap

- [ ] Snowflake connector
- [ ] Power BI integration
- [ ] Real-time streaming validation
- [ ] ML-based anomaly detection
- [ ] Data lineage tracking
- [ ] Collaborative rule editing
- [ ] Natural language query interface
