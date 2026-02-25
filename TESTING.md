# AI Data Quality Agent - Testing Guide

This guide covers all testing scenarios for the AI Data Quality Agent, including local database setup, LLM provider testing, and validation workflows.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Test Data Setup](#test-data-setup)
3. [Test Runner (CLI)](#test-runner-cli)
4. [Test Frontend](#test-frontend)
5. [LLM Provider Configuration](#llm-provider-configuration)
6. [Data Source Testing](#data-source-testing)
7. [Validation Modes](#validation-modes)
8. [Report Analysis](#report-analysis)

---

## Quick Start

### 1. Setup Test Database

```bash
cd /mnt/okcomputer/output/ai-data-quality-agent

# Setup test database with sample data
python test_data/setup_test_db.py
```

This creates:
- **SQLite database** (`test_data/test_database.db`) with 4 tables
- **Structured data**: CSV exports
- **Semi-structured data**: JSON logs, XML configs
- **Unstructured data**: Support tickets, product reviews
- **ADLS Gen2 mock**: File system structure

### 2. Run Interactive Test

```bash
# Interactive mode (recommended for first run)
python test_runner.py
```

### 3. Launch Test Frontend

```bash
cd test_frontend
npm install
npm run dev

# Open http://localhost:5174
```

---

## Test Data Setup

### Database Schema

The test database includes intentional quality issues for testing:

#### `customers` (1,000 rows)
| Column | Issues Injected |
|--------|-----------------|
| email | Null values, invalid formats, duplicates |
| phone | Invalid formats, missing country codes |
| date_of_birth | Future dates, extreme ages |
| country | Empty strings |
| status | Invalid values |
| lifetime_value | Negative values, extreme outliers |

#### `orders` (5,000 rows)
| Column | Issues Injected |
|--------|-----------------|
| customer_id | Non-existent references |
| product_id | Non-existent references |
| total_amount | Calculation errors |

#### `products` (200 rows)
| Column | Issues Injected |
|--------|-----------------|
| price | Negative values, zeros |
| stock_quantity | Negative values |

#### `sales_transactions` (10,000 rows)
| Column | Issues Injected |
|--------|-----------------|
| order_id | Non-existent references |
| fraud_score | Out of range (0-1) |
| currency | Invalid values |

### Data Types

```
test_data/
├── test_database.db          # Main SQLite database
├── structured/               # CSV exports
│   ├── customers_sample.csv
│   ├── orders_sample.csv
│   └── products.csv
├── semi_structured/          # JSON/XML files
│   ├── application_logs.json    (1,000 entries)
│   ├── user_events.json         (2,000 events)
│   └── system_configs.xml       (50 configs)
├── unstructured/             # Text documents
│   ├── support_tickets.json     (500 tickets)
│   └── product_reviews.json     (1,000 reviews)
├── adls_mock/                # ADLS Gen2 structure
│   ├── raw-data/
│   ├── processed-data/
│   ├── curated-data/
│   └── archive/
└── statistics.json           # Database stats
```

---

## Test Runner (CLI)

### Interactive Mode

```bash
python test_runner.py
```

Flow:
1. **Select LLM Provider** → Ollama / LM Studio / OpenAI / Anthropic
2. **Select Model** → Provider-specific models
3. **Configure Connection** → Base URL / API Key
4. **Select Data Type** → Structured / Semi-structured / Unstructured / ADLS
5. **Browse Resources** → Tables / Files / Folders
6. **Select Validation Mode** → Custom / AI / Hybrid
7. **Run Validation**
8. **View Power BI Style Report**

### Quick Test Mode

```bash
# Test with Ollama
python test_runner.py \
  --provider ollama \
  --model llama3.2 \
  --target customers \
  --mode hybrid

# Test with OpenAI
python test_runner.py \
  --provider openai \
  --model gpt-4 \
  --api-key sk-... \
  --target orders \
  --mode ai_recommended

# Test with LM Studio
python test_runner.py \
  --provider lmstudio \
  --base-url http://localhost:1234/v1 \
  --target products \
  --sample-size 500

# Test semi-structured data
python test_runner.py \
  --provider ollama \
  --source-type json \
  --target "test_data/semi_structured/application_logs.json"
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--provider` | LLM provider (ollama/lmstudio/openai/anthropic) | Required |
| `--model` | Model name | Required |
| `--base-url` | Base URL for local providers | Provider default |
| `--api-key` | API key for cloud providers | None |
| `--source-type` | Data source type | sqlite |
| `--target` | Target table/file | customers |
| `--mode` | Validation mode | hybrid |
| `--sample-size` | Sample size | 1000 |
| `--output-dir` | Report output directory | ./test_results |
| `--setup-db` | Setup test database | False |

---

## Test Frontend

### Features

The test frontend (`http://localhost:5174`) provides:

#### 1. LLM Provider Selection
- Visual provider cards
- Model selection dropdown
- Base URL configuration
- API key input (for cloud providers)
- Connection test button

#### 2. Data Source Browser
- **Structured**: Database tables with row/column counts
- **Semi-structured**: JSON/XML files with sizes
- **Unstructured**: Text documents
- **ADLS Gen2 Mock**: Container/folder hierarchy
- Expandable folder tree

#### 3. Validation Configuration
- Mode selection (Custom/AI/Hybrid)
- Sample size slider
- Configuration summary

#### 4. Results Dashboard
- Quality score gauge
- Pass/Fail/Warning stats
- Detailed results table
- AI-recommended solutions
- Apply/View detail buttons

### Usage

```bash
cd test_frontend
npm install
npm run dev
```

Navigate through steps:
1. **LLM** → Select provider and model
2. **Data** → Browse and select resource
3. **Config** → Set validation mode
4. **Results** → View Power BI style report

---

## LLM Provider Configuration

### Ollama (Recommended for Local Testing)

```bash
# Install Ollama
# https://ollama.com

# Pull models
ollama pull llama3.2
ollama pull mistral
ollama pull codellama

# Start Ollama (runs on localhost:11434)
ollama serve
```

**Test Configuration:**
- Provider: `ollama`
- Base URL: `http://localhost:11434`
- Models: `llama3.2`, `mistral`, `codellama`, `phi3`

### LM Studio

```bash
# Download LM Studio
# https://lmstudio.ai

# 1. Download a model (e.g., Llama 3.2)
# 2. Start the local server
# 3. Server runs on localhost:1234/v1 by default
```

**Test Configuration:**
- Provider: `lmstudio`
- Base URL: `http://localhost:1234/v1`
- Model: Auto-detected from loaded model

### OpenAI

**Test Configuration:**
- Provider: `openai`
- API Key: Your OpenAI API key
- Models: `gpt-4`, `gpt-4-turbo`, `gpt-3.5-turbo`

### Anthropic Claude

**Test Configuration:**
- Provider: `anthropic`
- API Key: Your Anthropic API key
- Models: `claude-3-5-sonnet`, `claude-3-opus`, `claude-3-haiku`

---

## Data Source Testing

### Structured Data (Database)

```bash
# Test customers table
python test_runner.py --target customers

# Test orders with referential integrity issues
python test_runner.py --target orders --mode hybrid

# Test high-volume transactions
python test_runner.py --target sales_transactions --sample-size 5000
```

### Semi-Structured Data

```bash
# Test JSON logs
python test_runner.py \
  --source-type json \
  --target "test_data/semi_structured/application_logs.json"

# Test XML configs
python test_runner.py \
  --source-type xml \
  --target "test_data/semi_structured/system_configs.xml"
```

### Unstructured Data

```bash
# Test support tickets
python test_runner.py \
  --source-type json \
  --target "test_data/unstructured/support_tickets.json"
```

### ADLS Gen2 Mock

```bash
# Test entire container
python test_runner.py \
  --source-type local_file \
  --target "test_data/adls_mock/raw-data"

# Test specific folder
python test_runner.py \
  --source-type local_file \
  --target "test_data/adls_mock/raw-data/customers"
```

---

## Validation Modes

### 1. Custom Rules Only

Uses only predefined validation rules from the rules database.

```bash
python test_runner.py --mode custom_rules --target customers
```

**Use case:** When you have established business rules and want consistent validation.

### 2. AI Recommended

LLM analyzes data profile and generates validation rules automatically.

```bash
python test_runner.py --mode ai_recommended --target orders
```

**Use case:** Exploring new datasets, discovering unknown quality issues.

### 3. Hybrid (Recommended)

Combines custom rules with AI-generated recommendations.

```bash
python test_runner.py --mode hybrid --target products
```

**Use case:** Best of both worlds - established rules + AI discovery.

---

## Report Analysis

### Power BI Style Report

The test runner generates interactive HTML reports with:

#### 1. Quality Score Gauge
- Visual donut chart
- Color-coded (Green ≥90%, Yellow 70-89%, Red <70%)
- Overall percentage

#### 2. KPI Cards
- Overall Quality Score
- Rules Passed
- Rules Failed
- Records Processed

#### 3. Validation Results Table
- Rule name and type
- Severity (Critical/Warning/Info)
- Status (Passed/Failed/Warning)
- Failed record count
- Action buttons

#### 4. AI-Recommended Solutions

**Auto-Apply Solutions:**
- One-click fixes
- Safe transformations
- Example: Standardize phone formats

**User Review Solutions:**
- Require manual approval
- Complex transformations
- Example: Merge duplicate records

### Sample Report Output

```
test_results/
└── report_customers_20240211_143022.html
```

Open in browser to view interactive dashboard.

---

## Docker Testing

### Full Stack

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f backend

# Run test
docker-compose exec backend python test_runner.py --setup-db
```

### Individual Services

```bash
# PostgreSQL
docker run -d \
  --name test-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15

# Redis
docker run -d \
  --name test-redis \
  -p 6379:6379 \
  redis:7
```

---

## Troubleshooting

### LLM Connection Issues

**Ollama not responding:**
```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# Restart Ollama
ollama serve
```

**LM Studio connection refused:**
```bash
# Check server is started in LM Studio UI
# Verify port in Settings > Local Inference Server
```

### Database Issues

**SQLite database not found:**
```bash
# Regenerate test database
python test_data/setup_test_db.py
```

### Frontend Issues

**Port already in use:**
```bash
# Use different port
npm run dev -- --port 5175
```

---

## Test Checklist

### Basic Functionality
- [ ] Setup test database
- [ ] Test Ollama connection
- [ ] Run validation on customers table
- [ ] View generated report
- [ ] Test all validation modes

### LLM Providers
- [ ] Test Ollama (llama3.2)
- [ ] Test LM Studio
- [ ] Test OpenAI (if key available)
- [ ] Test Anthropic (if key available)

### Data Sources
- [ ] Structured: All 4 tables
- [ ] Semi-structured: JSON logs
- [ ] Semi-structured: XML configs
- [ ] Unstructured: Support tickets
- [ ] ADLS mock: Browse containers

### Features
- [ ] Custom rules validation
- [ ] AI-recommended rules
- [ ] Hybrid mode
- [ ] Power BI style report
- [ ] AI solutions (auto-apply)
- [ ] AI solutions (user review)

---

## Performance Testing

### Sample Size Testing

```bash
# Small sample
python test_runner.py --sample-size 100

# Medium sample
python test_runner.py --sample-size 1000

# Large sample
python test_runner.py --sample-size 10000
```

### Concurrent Testing

```bash
# Run multiple validations simultaneously
for table in customers orders products; do
  python test_runner.py --target $table &
done
wait
```

---

## Continuous Integration

### GitHub Actions Example

```yaml
name: Test Data Quality Agent

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install Ollama
        run: |
          curl -fsSL https://ollama.com/install.sh | sh
          ollama pull llama3.2
      
      - name: Setup Test Database
        run: python test_data/setup_test_db.py
      
      - name: Run Tests
        run: |
          python test_runner.py \
            --provider ollama \
            --model llama3.2 \
            --target customers \
            --mode hybrid
```

---

## Next Steps

1. **Explore Different LLMs**: Compare results across providers
2. **Test Custom Rules**: Add your own validation rules
3. **Large Datasets**: Test with production-scale data
4. **Integration**: Connect to real ADLS Gen2, Databricks
5. **Automation**: Schedule regular validations

For production deployment, see [README.md](README.md).
