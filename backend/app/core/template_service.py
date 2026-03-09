import json
import os
import logging
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("uploads/templates")

class TemplateService:
    @staticmethod
    def _ensure_dir():
        if not TEMPLATE_DIR.exists():
            TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
            # Create a sample template if none exist
            TemplateService.create_sample_templates()

    @staticmethod
    def create_sample_templates():
        samples = [
            {
                "id": "customers",
                "name": "Customers",
                "description": "Enterprise customer master data schema.",
                "columns": [
                    {"name": "customer_id", "type": "INTEGER", "description": "Primary Key"},
                    {"name": "email", "type": "TEXT", "description": "Email address"},
                    {"name": "first_name", "type": "TEXT", "description": "First name"},
                    {"name": "last_name", "type": "TEXT", "description": "Last name"},
                    {"name": "phone", "type": "TEXT", "description": "Phone number"},
                    {"name": "date_of_birth", "type": "DATE", "description": "Date of birth"},
                    {"name": "country", "type": "TEXT", "description": "Country of origin"},
                    {"name": "created_at", "type": "TIMESTAMP", "description": "Registration time"},
                    {"name": "status", "type": "TEXT", "description": "Account status"},
                    {"name": "lifetime_value", "type": "REAL", "description": "Calculated LTV"}
                ]
            },
            {
                "id": "orders",
                "name": "Orders",
                "description": "Standard e-commerce order schema.",
                "columns": [
                    {"name": "order_id", "type": "INTEGER", "description": "Primary Key"},
                    {"name": "customer_id", "type": "INTEGER", "description": "Foreign Key to Customers"},
                    {"name": "product_id", "type": "INTEGER", "description": "Foreign Key to Products"},
                    {"name": "quantity", "type": "INTEGER", "description": "Order quantity"},
                    {"name": "unit_price", "type": "REAL", "description": "Price per unit"},
                    {"name": "total_amount", "type": "REAL", "description": "Total order value"},
                    {"name": "order_date", "type": "TIMESTAMP", "description": "Time of order"},
                    {"name": "shipping_address", "type": "TEXT", "description": "Delivery address"},
                    {"name": "status", "type": "TEXT", "description": "Order status (Pending, Shipped, etc.)"},
                    {"name": "discount_code", "type": "TEXT", "description": "Applied promo code"}
                ]
            },
            {
                "id": "products",
                "name": "Products",
                "description": "Product catalog schema.",
                "columns": [
                    {"name": "product_id", "type": "INTEGER", "description": "Primary Key"},
                    {"name": "product_name", "type": "TEXT", "description": "Name of product"},
                    {"name": "category", "type": "TEXT", "description": "Product category"},
                    {"name": "price", "type": "REAL", "description": "Current selling price"},
                    {"name": "cost", "type": "REAL", "description": "Acquisition cost"},
                    {"name": "stock_quantity", "type": "INTEGER", "description": "Current inventory count"},
                    {"name": "supplier_id", "type": "INTEGER", "description": "Supplier identifier"},
                    {"name": "created_at", "type": "TIMESTAMP", "description": "Time added to catalog"},
                    {"name": "is_active", "type": "BOOLEAN", "description": "Availability status"}
                ]
            },
            {
                "id": "sales_transactions",
                "name": "Sales Transactions",
                "description": "High-volume sales transaction logging.",
                "columns": [
                    {"name": "transaction_id", "type": "INTEGER", "description": "Primary Key"},
                    {"name": "order_id", "type": "INTEGER", "description": "Order reference"},
                    {"name": "payment_method", "type": "TEXT", "description": "Payment gateway/method"},
                    {"name": "payment_status", "type": "TEXT", "description": "Status of payment"},
                    {"name": "transaction_amount", "type": "REAL", "description": "Amount processed"},
                    {"name": "transaction_date", "type": "TIMESTAMP", "description": "Time of transaction"},
                    {"name": "currency", "type": "TEXT", "description": "Transaction currency"},
                    {"name": "fraud_score", "type": "REAL", "description": "AI-generated fraud risk score"}
                ]
            }
        ]
        
        for s in samples:
            path = TEMPLATE_DIR / f"{s['id']}.json"
            if not path.exists():
                with open(path, "w") as f:
                    json.dump(s, f, indent=4)

    @staticmethod
    def list_templates() -> List[Dict]:
        TemplateService._ensure_dir()
        templates = []
        for file in TEMPLATE_DIR.glob("*.json"):
            try:
                with open(file, "r") as f:
                    templates.append(json.load(f))
            except Exception as e:
                logger.error(f"Failed to load template {file}: {e}")
        return templates

    @staticmethod
    def get_template(template_id: str) -> Optional[Dict]:
        TemplateService._ensure_dir()
        path = TEMPLATE_DIR / f"{template_id}.json"
        if path.exists():
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to read template {template_id}: {e}")
        return None

    @staticmethod
    def save_template(template: Dict) -> bool:
        TemplateService._ensure_dir()
        if "id" not in template:
            return False
        path = TEMPLATE_DIR / f"{template['id']}.json"
        try:
            with open(path, "w") as f:
                json.dump(template, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Failed to save template {template['id']}: {e}")
            return False

    @staticmethod
    def delete_template(template_id: str) -> bool:
        TemplateService._ensure_dir()
        path = TEMPLATE_DIR / f"{template_id}.json"
        if path.exists():
            try:
                path.unlink()
                return True
            except Exception as e:
                logger.error(f"Failed to delete template {template_id}: {e}")
                return False
        return False
