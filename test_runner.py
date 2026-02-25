#!/usr/bin/env python3
"""
AI Data Quality Agent - Test Runner

Interactive test runner for validating the data quality agent with:
- LLM provider selection (Ollama, LM Studio, OpenAI, Anthropic)
- Model selection per provider
- Data source selection (Database tables, ADLS files)
- Validation mode selection
- Power BI style report generation
- Click-and-apply AI solutions
"""
import os
import sys
import json
import asyncio
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from app.agents.data_quality_agent import get_data_quality_agent
from app.agents.state import ValidationMode, DataSourceInfo, ValidationRule
from app.agents.llm_service import get_llm_service
from app.connectors.factory import ConnectorFactory
from app.core.config import Settings

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "test_data"
DB_PATH = TEST_DATA_DIR / "test_database.db"

# ✅ ADD THIS RIGHT HERE:
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@dataclass
class TestConfig:
    """Test configuration."""
    llm_provider: str = "ollama"
    llm_model: str = "llama3.2"
    llm_base_url: Optional[str] = None
    api_key: Optional[str] = None
    data_source_type: str = "sqlite"
    target_path: str = "customers"
    validation_mode: str = "hybrid"
    sample_size: int = 1000
    output_dir: str = "./test_results"


class LLMConfigurator:
    """Interactive LLM configuration."""
    
    PROVIDERS = {
        "1": {"name": "ollama", "display": "Ollama (Local)", "needs_key": False},
        "2": {"name": "lmstudio", "display": "LM Studio (Local)", "needs_key": False},
        "3": {"name": "openai", "display": "OpenAI (Cloud)", "needs_key": True},
        "4": {"name": "anthropic", "display": "Anthropic Claude (Cloud)", "needs_key": True},
    }
    
    MODELS = {
        "ollama": [
            ("llama3.2", "Llama 3.2 (Recommended)"),
            ("llama3.1", "Llama 3.1"),
            ("mistral", "Mistral"),
            ("codellama", "CodeLlama"),
            ("phi3", "Phi-3"),
            ("gemma2", "Gemma 2"),
        ],
        "lmstudio": [
            ("local-model", "Auto-detect from LM Studio"),
        ],
        "openai": [
            ("gpt-4", "GPT-4"),
            ("gpt-4-turbo", "GPT-4 Turbo"),
            ("gpt-3.5-turbo", "GPT-3.5 Turbo"),
        ],
        "anthropic": [
            ("claude-3-5-sonnet-20241022", "Claude 3.5 Sonnet (Recommended)"),
            ("claude-3-opus-20240229", "Claude 3 Opus"),
            ("claude-3-sonnet-20240229", "Claude 3 Sonnet"),
            ("claude-3-haiku-20240307", "Claude 3 Haiku"),
        ],
    }
    
    DEFAULT_URLS = {
        "ollama": "http://localhost:11434",
        "lmstudio": "http://localhost:1234/v1",
    }
    
    @classmethod
    def interactive_select(cls) -> Dict[str, str]:
        """Interactive LLM provider and model selection."""
        print("\n" + "=" * 60)
        print("LLM Provider Selection")
        print("=" * 60)
        
        # Show provider options
        for key, provider in cls.PROVIDERS.items():
            print(f"  {key}. {provider['display']}")
        
        # Get provider selection
        while True:
            choice = input("\nSelect provider (1-4): ").strip()
            if choice in cls.PROVIDERS:
                break
            print("Invalid choice. Please try again.")
        
        provider = cls.PROVIDERS[choice]
        provider_name = provider["name"]
        
        print(f"\n✓ Selected: {provider['display']}")
        
        # Get base URL for local providers
        base_url = None
        if provider_name in cls.DEFAULT_URLS:
            default_url = cls.DEFAULT_URLS[provider_name]
            url_input = input(f"Base URL [{default_url}]: ").strip()
            base_url = url_input if url_input else default_url
            
            # Test connection
            print(f"\n🔍 Testing connection to {base_url}...")
            # Connection test will happen later
        
        # Get API key for cloud providers
        api_key = None
        if provider["needs_key"]:
            api_key = input(f"Enter {provider['display']} API key: ").strip()
            if not api_key:
                print("⚠️  Warning: No API key provided. Connection may fail.")
        
        # Show model options
        print(f"\n📋 Available Models for {provider['display']}:")
        models = cls.MODELS.get(provider_name, [])
        for i, (model_id, model_name) in enumerate(models, 1):
            print(f"  {i}. {model_name}")
        
        # Get model selection
        while True:
            model_choice = input(f"\nSelect model (1-{len(models)}): ").strip()
            try:
                model_idx = int(model_choice) - 1
                if 0 <= model_idx < len(models):
                    selected_model = models[model_idx][0]
                    break
            except ValueError:
                pass
            print("Invalid choice. Please try again.")
        
        print(f"\n✓ Selected model: {selected_model}")
        
        return {
            "provider": provider_name,
            "model": selected_model,
            "base_url": base_url,
            "api_key": api_key,
        }


class DataSourceSelector:
    """Interactive data source selection."""
    
    DATA_TYPES = {
        "1": {"name": "structured", "display": "Structured (Database Tables)"},
        "2": {"name": "semi_structured", "display": "Semi-Structured (JSON/XML)"},
        "3": {"name": "unstructured", "display": "Unstructured (Text/Documents)"},
        "4": {"name": "adls_mock", "display": "ADLS Gen2 Mock (File System)"},
    }
    
    DB_TABLES = ["customers", "orders", "products", "sales_transactions"]
    
    @classmethod
    def interactive_select(cls) -> Dict[str, str]:
        """Interactive data source selection."""
        print("\n" + "=" * 60)
        print("📂 Data Source Selection")
        print("=" * 60)
        
        # Show data type options
        for key, data_type in cls.DATA_TYPES.items():
            print(f"  {key}. {data_type['display']}")
        
        # Get data type selection
        while True:
            choice = input("\nSelect data type (1-4): ").strip()
            if choice in cls.DATA_TYPES:
                break
            print("Invalid choice. Please try again.")
        
        data_type = cls.DATA_TYPES[choice]["name"]
        
        # Show available resources based on type
        if data_type == "structured":
            return cls._select_database_table()
        elif data_type == "semi_structured":
            return cls._select_semi_structured()
        elif data_type == "unstructured":
            return cls._select_unstructured()
        elif data_type == "adls_mock":
            return cls._select_adls_mock()
        
        return {"type": data_type, "path": ""}
    
    @classmethod
    def _select_database_table(cls) -> Dict[str, str]:
        """Select database table."""
        print("\nAvailable Database Tables:")
        
        # Show table statistics
        stats_path = TEST_DATA_DIR / "statistics.json"
        if stats_path.exists():
            with open(stats_path) as f:
                stats = json.load(f)
            
            for i, (table_name, table_stats) in enumerate(stats["tables"].items(), 1):
                row_count = table_stats["row_count"]
                col_count = len(table_stats["columns"])
                print(f"  {i}. {table_name} ({row_count:,} rows, {col_count} columns)")
                
                # Show column preview
                col_names = [c["name"] for c in table_stats["columns"][:5]]
                print(f"     Columns: {', '.join(col_names)}{'...' if len(table_stats['columns']) > 5 else ''}")
        else:
            for i, table in enumerate(cls.DB_TABLES, 1):
                print(f"  {i}. {table}")
        
        while True:
            choice = input(f"\nSelect table (1-{len(stats.get('tables', cls.DB_TABLES))}): ").strip()
            try:
                tables = list(stats["tables"].keys()) if stats else cls.DB_TABLES
                table_idx = int(choice) - 1
                if 0 <= table_idx < len(tables):
                    selected_table = tables[table_idx]
                    break
            except (ValueError, NameError):
                pass
            print("Invalid choice. Please try again.")
        
        print(f"\n✓ Selected table: {selected_table}")
        
        return {
            "type": "sqlite",
            "path": selected_table,
            "display": f"Table: {selected_table}"
        }
    
    @classmethod
    def _select_semi_structured(cls) -> Dict[str, str]:
        """Select semi-structured data file."""
        semi_dir = TEST_DATA_DIR / "semi_structured"
        files = list(semi_dir.glob("*.json")) + list(semi_dir.glob("*.xml"))
        
        print("\n📄 Available Semi-Structured Files:")
        for i, file in enumerate(files, 1):
            size_kb = file.stat().st_size / 1024
            print(f"  {i}. {file.name} ({size_kb:.1f} KB)")
        
        while True:
            choice = input(f"\nSelect file (1-{len(files)}): ").strip()
            try:
                file_idx = int(choice) - 1
                if 0 <= file_idx < len(files):
                    selected_file = files[file_idx]
                    break
            except ValueError:
                pass
            print("Invalid choice. Please try again.")
        
        print(f"\n✓ Selected file: {selected_file.name}")
        
        return {
            "type": "json" if selected_file.suffix == ".json" else "xml",
            "path": str(selected_file),
            "display": f"File: {selected_file.name}"
        }
    
    @classmethod
    def _select_unstructured(cls) -> Dict[str, str]:
        """Select unstructured data file."""
        unstruct_dir = TEST_DATA_DIR / "unstructured"
        files = list(unstruct_dir.glob("*.json"))
        
        print("\n📝 Available Unstructured Data Files:")
        for i, file in enumerate(files, 1):
            size_kb = file.stat().st_size / 1024
            print(f"  {i}. {file.name} ({size_kb:.1f} KB)")
        
        while True:
            choice = input(f"\nSelect file (1-{len(files)}): ").strip()
            try:
                file_idx = int(choice) - 1
                if 0 <= file_idx < len(files):
                    selected_file = files[file_idx]
                    break
            except ValueError:
                pass
            print("Invalid choice. Please try again.")
        
        print(f"\n✓ Selected file: {selected_file.name}")
        
        return {
            "type": "json",
            "path": str(selected_file),
            "display": f"File: {selected_file.name}"
        }
    
    @classmethod
    def _select_adls_mock(cls) -> Dict[str, str]:
        """Select from ADLS mock structure."""
        adls_dir = TEST_DATA_DIR / "adls_mock"
        
        # Show containers
        containers = [d for d in adls_dir.iterdir() if d.is_dir()]
        
        print("\n☁️  ADLS Gen2 Mock - Containers:")
        for i, container in enumerate(containers, 1):
            file_count = len(list(container.rglob("*")))
            print(f"  {i}. {container.name}/ ({file_count} items)")
        
        while True:
            choice = input(f"\nSelect container (1-{len(containers)}): ").strip()
            try:
                cont_idx = int(choice) - 1
                if 0 <= cont_idx < len(containers):
                    selected_container = containers[cont_idx]
                    break
            except ValueError:
                pass
            print("Invalid choice. Please try again.")
        
        # Show files in container
        files = list(selected_container.rglob("*.csv")) + list(selected_container.rglob("*.parquet"))
        if not files:
            files = list(selected_container.rglob("*"))
        
        print(f"\n📁 Files in {selected_container.name}:")
        for i, file in enumerate(files[:10], 1):  # Show first 10
            rel_path = file.relative_to(selected_container)
            size_kb = file.stat().st_size / 1024 if file.is_file() else 0
            print(f"  {i}. {rel_path} ({size_kb:.1f} KB)")
        
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more files")
        
        file_choice = input(f"\nSelect file (1-{min(len(files), 10)}), or 'all' for entire container: ").strip()
        
        if file_choice.lower() == 'all':
            selected_path = str(selected_container)
        else:
            try:
                file_idx = int(file_choice) - 1
                selected_path = str(files[file_idx])
            except (ValueError, IndexError):
                selected_path = str(selected_container)
        
        print(f"\n✓ Selected: {selected_path}")
        
        return {
            "type": "local_file",
            "path": selected_path,
            "display": f"ADLS: {selected_container.name}"
        }


class ValidationModeSelector:
    """Interactive validation mode selection."""
    
    MODES = {
        "1": {"mode": "custom_rules", "display": "Custom Rules Only", "description": "Use only predefined validation rules"},
        "2": {"mode": "ai_recommended", "display": "AI Recommended", "description": "Let AI generate rules based on data profiling"},
        "3": {"mode": "hybrid", "display": "Hybrid (Recommended)", "description": "Combine custom rules with AI recommendations"},
    }
    
    @classmethod
    def interactive_select(cls) -> str:
        """Interactive validation mode selection."""
        print("\n" + "=" * 60)
        print("Validation Mode Selection")
        print("=" * 60)
        
        for key, mode in cls.MODES.items():
            print(f"\n  {key}. {mode['display']}")
            print(f"     {mode['description']}")
        
        while True:
            choice = input("\nSelect mode (1-3): ").strip()
            if choice in cls.MODES:
                break
            print("Invalid choice. Please try again.")
        
        selected_mode = cls.MODES[choice]["mode"]
        print(f"\n✓ Selected mode: {cls.MODES[choice]['display']}")
        
        return selected_mode


class PowerBIStyleReport:
    """Generate Power BI style HTML report."""
    
    def __init__(self, validation_result: Dict[str, Any], config: TestConfig):
        self.result = validation_result
        self.config = config
        self.timestamp = datetime.now()
        
        # Merge summary_report sub-dict into top-level result for easy access
        summary = self.result.get('summary_report')
        if isinstance(summary, dict):
            for key in ['total_rules', 'passed_rules', 'failed_rules', 'warning_rules',
                        'critical_issues', 'records_processed', 'execution_time_ms']:
                if key not in self.result and key in summary:
                    self.result[key] = summary[key]
        
        # Extract AI solutions from messages if not already present
        if not self.result.get('solutions'):
            solutions = []
            for msg in self.result.get('messages', []):
                if isinstance(msg, dict) and msg.get('role') == 'assistant':
                    content = msg.get('content', '')
                    if content and len(content) > 50:
                        solutions.append({
                            'title': 'AI Analysis & Recommendations',
                            'description': content,
                            'impact': 'Review AI recommendations for data quality improvements',
                            'auto_apply': False,
                        })
            self.result['solutions'] = solutions
    
    def generate_html(self) -> str:
        """Generate Power BI style HTML report."""
        quality_score = self.result.get('quality_score') or 0
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Report - {self.config.target_path}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f3f2f1;
            color: #323130;
        }}
        .header {{
            background: linear-gradient(135deg, #0078d4 0%, #106ebe 100%);
            color: white;
            padding: 20px 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{ font-size: 24px; font-weight: 600; }}
        .header .subtitle {{ font-size: 14px; opacity: 0.9; margin-top: 5px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        .kpi-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .kpi-card {{
            background: white;
            border-radius: 4px;
            padding: 20px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1);
        }}
        .kpi-card .label {{ font-size: 12px; color: #605e5c; text-transform: uppercase; }}
        .kpi-card .value {{ 
            font-size: 32px; 
            font-weight: 600; 
            margin-top: 5px;
            color: {self._get_score_color(quality_score)};
        }}
        .kpi-card .trend {{ font-size: 12px; margin-top: 5px; }}
        .section {{
            background: white;
            border-radius: 4px;
            margin-bottom: 20px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1);
        }}
        .section-header {{
            padding: 15px 20px;
            border-bottom: 1px solid #edebe9;
            font-size: 16px;
            font-weight: 600;
        }}
        .section-body {{ padding: 20px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        th {{
            text-align: left;
            padding: 10px;
            background: #f3f2f1;
            font-weight: 600;
            color: #323130;
        }}
        td {{ padding: 10px; border-bottom: 1px solid #edebe9; }}
        tr:hover {{ background: #f3f2f1; }}
        .status-passed {{ color: #107c10; font-weight: 600; }}
        .status-failed {{ color: #a80000; font-weight: 600; }}
        .status-warning {{ color: #ffc107; font-weight: 600; }}
        .severity-critical {{ background: #fde7e9; color: #a80000; padding: 2px 8px; border-radius: 12px; font-size: 11px; }}
        .severity-warning {{ background: #fff4ce; color: #8a6d04; padding: 2px 8px; border-radius: 12px; font-size: 11px; }}
        .severity-info {{ background: #e6f2ff; color: #0078d4; padding: 2px 8px; border-radius: 12px; font-size: 11px; }}
        .solution-card {{
            background: #f9f9f9;
            border-left: 4px solid #0078d4;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 0 4px 4px 0;
        }}
        .solution-card h4 {{ font-size: 14px; margin-bottom: 5px; }}
        .solution-card p {{ font-size: 12px; color: #605e5c; margin-bottom: 10px; }}
        .btn {{
            display: inline-block;
            padding: 6px 16px;
            border-radius: 2px;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
        }}
        .btn-primary {{ background: #0078d4; color: white; }}
        .btn-primary:hover {{ background: #106ebe; }}
        .btn-secondary {{ background: #f3f2f1; color: #323130; border: 1px solid #8a8886; }}
        .btn-secondary:hover {{ background: #e1dfdd; }}
        .chart-container {{
            height: 300px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .donut-chart {{
            width: 200px;
            height: 200px;
            border-radius: 50%;
            background: conic-gradient(
                #107c10 0deg {self._get_passed_angle()}deg,
                #a80000 {self._get_passed_angle()}deg {self._get_passed_angle() + self._get_failed_angle()}deg,
                #ffc107 {self._get_passed_angle() + self._get_failed_angle()}deg {self._get_passed_angle() + self._get_failed_angle() + self._get_warning_angle()}deg,
                #edebe9 {self._get_passed_angle() + self._get_failed_angle() + self._get_warning_angle()}deg 360deg
            );
            position: relative;
        }}
        .donut-chart::after {{
            content: '';
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 120px;
            height: 120px;
            background: white;
            border-radius: 50%;
        }}
        .donut-label {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            text-align: center;
            z-index: 1;
        }}
        .donut-label .score {{ font-size: 36px; font-weight: 600; color: {self._get_score_color(quality_score)}; }}
        .donut-label .label {{ font-size: 12px; color: #605e5c; }}
        .legend {{
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-top: 20px;
        }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 12px; }}
        .legend-color {{ width: 12px; height: 12px; border-radius: 2px; }}
        .filters {{
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }}
        .filter-btn {{
            padding: 5px 12px;
            border: 1px solid #8a8886;
            background: white;
            border-radius: 2px;
            font-size: 12px;
            cursor: pointer;
        }}
        .filter-btn.active {{ background: #0078d4; color: white; border-color: #0078d4; }}
        .metric-bar {{
            height: 8px;
            background: #edebe9;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 5px;
        }}
        .metric-bar-fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Report</h1>
        <div class="subtitle">
            {self.config.target_path} | Generated: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
    
    <div class="container">
        <!-- KPI Cards -->
        <div class="kpi-row">
            <div class="kpi-card">
                <div class="label">Overall Quality Score</div>
                <div class="value">{quality_score:.1f}%</div>
                <div class="trend">Based on {self.result.get('total_rules', 0)} validation rules</div>
            </div>
            <div class="kpi-card">
                <div class="label">Rules Passed</div>
                <div class="value" style="color: #107c10;">{self.result.get('passed_rules', 0)}</div>
                <div class="trend">{self._get_passed_percentage():.1f}% success rate</div>
            </div>
            <div class="kpi-card">
                <div class="label">Rules Failed</div>
                <div class="value" style="color: #a80000;">{self.result.get('failed_rules', 0)}</div>
                <div class="trend">Requires attention</div>
            </div>
            <div class="kpi-card">
                <div class="label">Records Processed</div>
                <div class="value" style="color: #0078d4;">{self.result.get('records_processed') or 0:,}</div>
                <div class="trend">Sample size: {self.config.sample_size:,}</div>
            </div>
        </div>
        
        <!-- Quality Overview Chart -->
        <div class="section">
            <div class="section-header">Quality Overview</div>
            <div class="section-body">
                <div class="chart-container">
                    <div style="text-align: center;">
                        <div class="donut-chart">
                            <div class="donut-label">
                                <div class="score">{quality_score:.0f}</div>
                                <div class="label">Quality Score</div>
                            </div>
                        </div>
                        <div class="legend">
                            <div class="legend-item">
                                <div class="legend-color" style="background: #107c10;"></div>
                                <span>Passed ({self.result.get('passed_rules', 0)})</span>
                            </div>
                            <div class="legend-item">
                                <div class="legend-color" style="background: #a80000;"></div>
                                <span>Failed ({self.result.get('failed_rules', 0)})</span>
                            </div>
                            <div class="legend-item">
                                <div class="legend-color" style="background: #ffc107;"></div>
                                <span>Warnings ({self.result.get('warning_rules', 0)})</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Validation Results Table -->
        <div class="section">
            <div class="section-header">Validation Results</div>
            <div class="section-body">
                <div class="filters">
                    <button class="filter-btn active">All</button>
                    <button class="filter-btn">Critical</button>
                    <button class="filter-btn">Failed</button>
                    <button class="filter-btn">Warnings</button>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Rule</th>
                            <th>Type</th>
                            <th>Severity</th>
                            <th>Status</th>
                            <th>Failed Records</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {self._generate_results_rows()}
                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- AI Solutions -->
        <div class="section">
            <div class="section-header">🤖 AI-Recommended Solutions</div>
            <div class="section-body">
                {self._generate_solutions()}
            </div>
        </div>
        
        <!-- Configuration Info -->
        <div class="section">
            <div class="section-header">Configuration</div>
            <div class="section-body">
                <table>
                    <tr><td><strong>LLM Provider</strong></td><td>{self.config.llm_provider}</td></tr>
                    <tr><td><strong>Model</strong></td><td>{self.config.llm_model}</td></tr>
                    <tr><td><strong>Data Source</strong></td><td>{self.config.data_source_type}</td></tr>
                    <tr><td><strong>Target</strong></td><td>{self.config.target_path}</td></tr>
                    <tr><td><strong>Validation Mode</strong></td><td>{self.config.validation_mode}</td></tr>
                    <tr><td><strong>Sample Size</strong></td><td>{self.config.sample_size:,}</td></tr>
                </table>
            </div>
        </div>
    </div>
</body>
</html>"""
        return html
    
    def _get_score_color(self, score: float) -> str:
        if score >= 90:
            return "#107c10"
        elif score >= 70:
            return "#ffc107"
        else:
            return "#a80000"
    
    def _get_passed_angle(self) -> float:
        total = self.result.get('total_rules', 1)
        passed = self.result.get('passed_rules', 0)
        return (passed / total) * 360 if total > 0 else 0
    
    def _get_failed_angle(self) -> float:
        total = self.result.get('total_rules', 1)
        failed = self.result.get('failed_rules', 0)
        return (failed / total) * 360 if total > 0 else 0

    def _get_warning_angle(self) -> float:
        total = self.result.get('total_rules', 1)
        warnings = self.result.get('warning_rules', 0)
        return (warnings / total) * 360 if total > 0 else 0
    
    def _get_passed_percentage(self) -> float:
        total = self.result.get('total_rules', 1)
        passed = self.result.get('passed_rules', 0)
        return (passed / total) * 100 if total > 0 else 0
    
    def _generate_results_rows(self) -> str:
        """Generate table rows for validation results."""
        results = self.result.get('validation_results', [])
        if not results:
            return '<tr><td colspan="6" style="text-align: center; color: #605e5c;">No validation results available</td></tr>'
        
        def _get(obj, key, default=None):
            """Get value from dict or dataclass."""
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)
        
        rows = []
        for result in results:
            status = _get(result, 'status', 'unknown') or 'unknown'
            severity = _get(result, 'severity', 'info') or 'info'
            rule_name = _get(result, 'rule_name', 'Unknown') or 'Unknown'
            rule_type = _get(result, 'rule_type', 'N/A') or 'N/A'
            failed_count = _get(result, 'failed_count', 0) or 0
            failure_pct = _get(result, 'failure_percentage', 0) or 0
            
            status_class = f"status-{status}"
            severity_class = f"severity-{severity}"
            
            rows.append(f"""
                <tr>
                    <td><strong>{rule_name}</strong></td>
                    <td>{rule_type}</td>
                    <td><span class="{severity_class}">{severity.upper()}</span></td>
                    <td class="{status_class}">{status.upper()}</td>
                    <td>{failed_count:,} ({failure_pct:.1f}%)</td>
                    <td><button class="btn btn-secondary" onclick="alert('View details for {rule_name}')">View</button></td>
                </tr>
            """)
        
        return ''.join(rows)
    
    def _generate_solutions(self) -> str:
        """Generate AI solution cards."""
        solutions = self.result.get('solutions', [])
        if not solutions:
            return '<p style="color: #605e5c;">No AI solutions available for this validation.</p>'
        
        cards = []
        for solution in solutions:
            auto_apply = solution.get('auto_apply', False)
            sol_title = solution.get('title', 'Solution')
            apply_btn = f'<button class="btn btn-primary" onclick="alert(\'Applying: {sol_title}\')">Apply Solution</button>' if auto_apply else ''
            cards.append(f"""
                <div class="solution-card">
                    <h4>{sol_title}</h4>
                    <p>{solution.get('description', '')}</p>
                    <p><strong>Impact:</strong> {solution.get('impact', 'N/A')}</p>
                    <div style="margin-top: 10px;">
                        {apply_btn}
                        <button class="btn btn-secondary" onclick="alert('View recommendation details')">View Details</button>
                    </div>
                </div>
            """)
        
        return ''.join(cards)
    
    def save(self, output_dir: str):
        """Save report to file."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Sanitize path for use as filename (remove drive letters, backslashes, etc.)
        safe_name = Path(self.config.target_path).name  # Just the last component
        safe_name = safe_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace(' ', '_')
        filename = f"report_{safe_name}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.html"
        filepath = output_path / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(self.generate_html())

        return filepath


async def run_validation(config: TestConfig) -> Dict[str, Any]:
    """Run validation with the given configuration."""
    
    # Override environment settings
    os.environ['LLM_PROVIDER'] = config.llm_provider
    os.environ[f'{config.llm_provider.upper()}_MODEL'] = config.llm_model
    
    if config.llm_base_url:
        os.environ[f'{config.llm_provider.upper()}_BASE_URL'] = config.llm_base_url
    if config.api_key:
        os.environ[f'{config.llm_provider.upper()}_API_KEY'] = config.api_key
    
    # Create data source info
    connection_config = {}
    if config.data_source_type == "sqlite":
        connection_config = {"connection_string": f"sqlite:///{DB_PATH}"}
    elif config.data_source_type in ["json", "xml"]:
        connection_config = {"base_path": str(Path(config.target_path).parent)}
    else:
        connection_config = {"base_path": config.target_path}
    
    data_source_info = DataSourceInfo(
        source_type=config.data_source_type,
        connection_config=connection_config,
        target_path=config.target_path,
    )
    
    # Run agent
    agent = get_data_quality_agent()
    validation_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\nStarting validation...")
    print(f"   Validation ID: {validation_id}")
    print(f"   Target: {config.target_path}")
    print(f"   Mode: {config.validation_mode}")
    
    result = await agent.run(
        validation_id=validation_id,
        validation_mode=ValidationMode(config.validation_mode),
        data_source_info=data_source_info,
        custom_rules=[],
        execution_config={"sample_size": config.sample_size},
    )
    
    return result


def interactive_mode():
    """Run interactive test mode."""
    print("=" * 70)
    print("AI Data Quality Agent - Interactive Test Mode")
    print("=" * 70)
    
    # Select LLM
    llm_config = LLMConfigurator.interactive_select()
    
    # Select data source
    data_source = DataSourceSelector.interactive_select()
    
    # Select validation mode
    validation_mode = ValidationModeSelector.interactive_select()
    
    # Get sample size
    sample_size = input("\nSample size [1000]: ").strip()
    sample_size = int(sample_size) if sample_size else 1000
    
    # Create config
    config = TestConfig(
        llm_provider=llm_config["provider"],
        llm_model=llm_config["model"],
        llm_base_url=llm_config["base_url"],
        api_key=llm_config["api_key"],
        data_source_type=data_source["type"],
        target_path=data_source["path"],
        validation_mode=validation_mode,
        sample_size=sample_size,
    )
    
    # Confirm
    print("\n" + "=" * 70)
    print("📋 Configuration Summary:")
    print("=" * 70)
    print(f"  LLM: {config.llm_provider} ({config.llm_model})")
    print(f"  Data Source: {data_source['display']}")
    print(f"  Validation Mode: {config.validation_mode}")
    print(f"  Sample Size: {config.sample_size:,}")
    
    confirm = input("\nProceed with validation? [Y/n]: ").strip().lower()
    if confirm and confirm != 'y':
        print("Cancelled.")
        return
    
    # Run validation
    result = asyncio.run(run_validation(config))
    
    # Generate report
    print("\nGenerating Power BI style report...")
    report = PowerBIStyleReport(result, config)
    report_path = report.save(config.output_dir)
    
    print(f"\nValidation complete!")
    quality_score = result.get('quality_score', 0)
    if quality_score is None:
        quality_score = 0
    print(f"   Quality Score: {quality_score:.1f}%")
    print(f"   Report saved: {report_path}")
    
    # Open report
    open_report = input("\nOpen report in browser? [Y/n]: ").strip().lower()
    if not open_report or open_report == 'y':
        import webbrowser
        webbrowser.open(f"file://{report_path.absolute()}")


def quick_test(args):
    """Run quick test with command line arguments."""
    config = TestConfig(
        llm_provider=args.provider,
        llm_model=args.model,
        data_source_type=args.source_type,
        target_path=args.target,
        validation_mode=args.mode,
        sample_size=args.sample_size,
    )
    
    result = asyncio.run(run_validation(config))
    
    # Generate report
    report = PowerBIStyleReport(result, config)
    report_path = report.save(config.output_dir)
    
    print(f"\nValidation complete!")
    quality_score = result.get('quality_score', 0)
    if quality_score is None:
        quality_score = 0
    print(f"   Quality Score: {quality_score:.1f}%")
    print(f"   Report saved: {report_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="AI Data Quality Agent - Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (recommended)
  python test_runner.py
  
  # Quick test with specific provider
  python test_runner.py --provider ollama --model llama3.2 --target customers
  
  # Test with OpenAI
  python test_runner.py --provider openai --model gpt-4 --target orders --mode hybrid
        """
    )
    
    parser.add_argument("--provider", choices=["ollama", "lmstudio", "openai", "anthropic"],
                       help="LLM provider")
    parser.add_argument("--model", help="Model name")
    parser.add_argument("--base-url", help="Base URL for local providers")
    parser.add_argument("--api-key", help="API key for cloud providers")
    parser.add_argument("--source-type", default="sqlite",
                       help="Data source type")
    parser.add_argument("--target", default="customers",
                       help="Target table/file")
    parser.add_argument("--mode", default="hybrid",
                       choices=["custom_rules", "ai_recommended", "hybrid"],
                       help="Validation mode")
    parser.add_argument("--sample-size", type=int, default=1000,
                       help="Sample size")
    parser.add_argument("--output-dir", default="./test_results",
                       help="Output directory for reports")
    parser.add_argument("--setup-db", action="store_true",
                       help="Setup test database before running")
    
    args = parser.parse_args()
    
    # Setup database if requested
    if args.setup_db:
        print("Setting up test database...")
        import subprocess
        subprocess.run([sys.executable, str(TEST_DATA_DIR / "setup_test_db.py")])
        return
    
    # Run in appropriate mode
    if args.provider and args.model:
        quick_test(args)
    else:
        interactive_mode()


if __name__ == "__main__":
    main()
