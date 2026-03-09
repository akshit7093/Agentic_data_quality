"""
Template Seeder  —  template_seed.py
=====================================
Seeds the four canonical business templates into the TemplateStore on
startup.  Already-existing templates (matched by **name**) are skipped so
user edits are preserved.

Wire it up in main.py / lifespan:

    from app.agents.template_seed import seed_builtin_templates

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await seed_builtin_templates()
        yield
"""

from __future__ import annotations
import logging
from app.agents.template_service import get_template_store

logger = logging.getLogger(__name__)

_SQL_TO_HINT: dict[str, str] = {
    "INTEGER": "int", "TEXT": "str", "REAL": "float",
    "BOOLEAN": "bool", "TIMESTAMP": "datetime", "DATE": "datetime",
}

def _col(name, sql_type, description="", required=False, aliases=None):
    return {
        "name": name,
        "dtype_hint": _SQL_TO_HINT.get(sql_type, "str"),
        "description": description,
        "required": required,
        "aliases": aliases or [],
    }

BUILTIN_TEMPLATES = [
    {
        "name": "Customers",
        "description": "Standard customer master table — 1 000 rows, 10 columns",
        "name_similarity_min": 0.65,
        "dtype_match_required": False,
        "columns": [
            _col("customer_id",    "INTEGER",   "Unique customer identifier",                         required=True,  aliases=["cust_id", "id", "customerid", "client_id"]),
            _col("email",          "TEXT",      "Customer email address",                             required=True,  aliases=["email_address", "mail", "e_mail"]),
            _col("first_name",     "TEXT",      "Customer first / given name",                        required=True,  aliases=["fname", "given_name", "firstname", "forename"]),
            _col("last_name",      "TEXT",      "Customer family / last name",                        required=True,  aliases=["lname", "surname", "lastname", "family_name"]),
            _col("phone",          "TEXT",      "Contact phone number",                                               aliases=["phone_number", "telephone", "mobile"]),
            _col("date_of_birth",  "DATE",      "Customer date of birth",                                             aliases=["dob", "birthdate", "birth_date", "birthday"]),
            _col("country",        "TEXT",      "Country of residence",                                               aliases=["country_code", "nation", "region"]),
            _col("created_at",     "TIMESTAMP", "Account creation timestamp (UTC)",                                   aliases=["created_date", "signup_date", "registration_date"]),
            _col("status",         "TEXT",      "Account status (active / inactive / suspended)",                     aliases=["account_status", "state", "active"]),
            _col("lifetime_value", "REAL",      "Total lifetime revenue (USD)",                                       aliases=["ltv", "clv", "total_spend", "lifetime_revenue"]),
        ],
    },
    {
        "name": "Orders",
        "description": "Standard orders / line-items table — 5 000 rows, 10 columns",
        "name_similarity_min": 0.65,
        "dtype_match_required": False,
        "columns": [
            _col("order_id",         "INTEGER",   "Unique order identifier",                           required=True,  aliases=["id", "orderid", "order_number", "order_ref"]),
            _col("customer_id",      "INTEGER",   "FK → customers.customer_id",                        required=True,  aliases=["cust_id", "customerid", "buyer_id"]),
            _col("product_id",       "INTEGER",   "FK → products.product_id",                          required=True,  aliases=["productid", "item_id", "sku_id", "sku"]),
            _col("quantity",         "INTEGER",   "Number of units ordered",                           required=True,  aliases=["qty", "units", "num_units"]),
            _col("unit_price",       "REAL",      "Price per unit at time of order (USD)",             required=True,  aliases=["price", "unit_cost", "item_price"]),
            _col("total_amount",     "REAL",      "Line total = quantity × unit_price",                required=True,  aliases=["total", "line_total", "order_total", "subtotal"]),
            _col("order_date",       "TIMESTAMP", "Order placement timestamp (UTC)",                   required=True,  aliases=["date", "placed_at", "created_at", "purchase_date"]),
            _col("shipping_address", "TEXT",      "Full delivery address (free-text)",                                 aliases=["address", "delivery_address", "ship_to"]),
            _col("status",           "TEXT",      "Order status (pending / shipped / delivered)",                     aliases=["order_status", "state", "fulfillment_status"]),
            _col("discount_code",    "TEXT",      "Promotional / coupon code applied",                                aliases=["coupon", "promo_code", "voucher", "coupon_code"]),
        ],
    },
    {
        "name": "Products",
        "description": "Product catalogue table — 200 rows, 9 columns",
        "name_similarity_min": 0.65,
        "dtype_match_required": False,
        "columns": [
            _col("product_id",     "INTEGER",   "Unique product identifier",                          required=True,  aliases=["id", "productid", "sku", "item_id"]),
            _col("product_name",   "TEXT",      "Display name of the product",                        required=True,  aliases=["name", "title", "item_name", "product"]),
            _col("category",       "TEXT",      "Product category / department",                                      aliases=["cat", "department", "type", "product_category"]),
            _col("price",          "REAL",      "Retail selling price (USD)",                         required=True,  aliases=["retail_price", "sale_price", "msrp", "unit_price"]),
            _col("cost",           "REAL",      "Wholesale / COGS cost (USD)",                                        aliases=["cogs", "cost_price", "unit_cost", "wholesale_price"]),
            _col("stock_quantity", "INTEGER",   "Current inventory on-hand (units)",                                  aliases=["stock", "inventory", "qty_on_hand", "quantity"]),
            _col("supplier_id",    "INTEGER",   "FK → suppliers.supplier_id",                                         aliases=["vendor_id", "supplierid", "manufacturer_id"]),
            _col("created_at",     "TIMESTAMP", "Product listing creation timestamp (UTC)",                           aliases=["created_date", "added_at", "date_added"]),
            _col("is_active",      "BOOLEAN",   "Whether the product is live / visible",                             aliases=["active", "enabled", "published", "is_published"]),
        ],
    },
    {
        "name": "Sales Transactions",
        "description": "Payment / transaction ledger — 10 000 rows, 8 columns",
        "name_similarity_min": 0.65,
        "dtype_match_required": False,
        "columns": [
            _col("transaction_id",     "INTEGER",   "Unique transaction identifier",                  required=True,  aliases=["txn_id", "id", "payment_id", "transactionid"]),
            _col("order_id",           "INTEGER",   "FK → orders.order_id",                           required=True,  aliases=["orderid", "order_ref", "reference_id"]),
            _col("payment_method",     "TEXT",      "Payment method used",                            required=True,  aliases=["method", "pay_method", "payment_type", "payment_mode"]),
            _col("payment_status",     "TEXT",      "Status of the payment",                          required=True,  aliases=["status", "txn_status", "result", "state"]),
            _col("transaction_amount", "REAL",      "Amount charged in transaction currency",         required=True,  aliases=["amount", "charge", "total", "txn_amount", "value"]),
            _col("transaction_date",   "TIMESTAMP", "UTC timestamp of the transaction",               required=True,  aliases=["date", "txn_date", "paid_at", "processed_at"]),
            _col("currency",           "TEXT",      "ISO 4217 currency code (USD / EUR / etc.)",                      aliases=["currency_code", "iso_currency", "ccy"]),
            _col("fraud_score",        "REAL",      "ML fraud risk score 0.0 – 1.0",                                 aliases=["risk_score", "fraud_probability", "fraud_risk", "risk"]),
        ],
    },
]


async def seed_builtin_templates() -> None:
    """Ensure all four built-in templates exist. Skips already-present names."""
    store = get_template_store()
    existing_names = {t["name"] for t in store.list_all()}
    created = 0
    for tpl in BUILTIN_TEMPLATES:
        if tpl["name"] in existing_names:
            logger.debug("Template '%s' already exists – skipping.", tpl["name"])
            continue
        store.create(
            name=tpl["name"],
            description=tpl["description"],
            columns=tpl["columns"],
            name_similarity_min=tpl["name_similarity_min"],
            dtype_match_required=tpl["dtype_match_required"],
        )
        logger.info("Seeded template: '%s' (%d cols)", tpl["name"], len(tpl["columns"]))
        created += 1
    logger.info("Template seeder done — %d new, %d already present.", created, len(existing_names))
