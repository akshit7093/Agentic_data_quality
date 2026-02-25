"""Setup test database with sample data for testing AI Data Quality Agent.

This script creates:
- Structured data: customers, orders, products tables
- Semi-structured data: JSON logs, XML configs
- Unstructured data: Text documents with metadata
- ADLS Gen2 mock: File system structure
"""
import sqlite3
import json
import random
import os
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

TEST_DATA_DIR = Path(__file__).parent
DB_PATH = TEST_DATA_DIR / "test_database.db"


def create_structured_tables(conn):
    """Create structured data tables with intentional quality issues."""
    cursor = conn.cursor()
    
    # Customers table with quality issues
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            phone TEXT,
            date_of_birth DATE,
            country TEXT,
            created_at TIMESTAMP,
            status TEXT,
            lifetime_value REAL
        )
    ''')
    
    # Orders table with referential integrity issues
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            order_date TIMESTAMP,
            shipping_address TEXT,
            status TEXT,
            discount_code TEXT
        )
    ''')
    
    # Products table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            price REAL,
            cost REAL,
            stock_quantity INTEGER,
            supplier_id INTEGER,
            created_at TIMESTAMP,
            is_active BOOLEAN
        )
    ''')
    
    # Sales transactions (high volume table)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_transactions (
            transaction_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            payment_method TEXT,
            payment_status TEXT,
            transaction_amount REAL,
            transaction_date TIMESTAMP,
            currency TEXT,
            fraud_score REAL
        )
    ''')
    
    conn.commit()


def generate_customers(conn, count=1000):
    """Generate customer data with intentional quality issues."""
    cursor = conn.cursor()
    
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Edward', 'Fiona', 
                   'George', 'Hannah', 'Ian', 'Julia', 'Kevin', 'Laura', 'Michael', 'Nancy']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson']
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia', 'Japan', 'India', 'Brazil', '']
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com', 'invalid']
    
    customers = []
    for i in range(1, count + 1):
        # Introduce quality issues intentionally
        has_issues = random.random() < 0.15  # 15% have issues
        
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Email issues: null, invalid format, duplicates
        if has_issues and random.random() < 0.3:
            email = None
        elif has_issues and random.random() < 0.3:
            email = f"invalid_email_{i}"
        else:
            email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"
        
        # Phone issues: invalid formats, nulls
        if has_issues and random.random() < 0.2:
            phone = None
        elif has_issues and random.random() < 0.3:
            phone = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
        else:
            phone = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # DOB issues: future dates, very old dates
        if has_issues and random.random() < 0.2:
            dob = datetime.now() + timedelta(days=random.randint(1, 365))  # Future date
        else:
            dob = datetime(1950, 1, 1) + timedelta(days=random.randint(0, 20000))
        
        # Country issues: empty strings
        country = random.choice(countries)
        
        # Status distribution
        status = random.choices(
            ['active', 'inactive', 'suspended', 'pending', ''],
            weights=[70, 15, 5, 8, 2]
        )[0]
        
        # LTV issues: negative values, extreme outliers
        if has_issues and random.random() < 0.1:
            ltv = -random.uniform(100, 1000)
        elif has_issues and random.random() < 0.1:
            ltv = random.uniform(100000, 1000000)  # Extreme outlier
        else:
            ltv = random.uniform(0, 50000)
        
        customers.append((
            i, email, first_name, last_name, phone, dob.date(), country,
            datetime.now() - timedelta(days=random.randint(0, 1000)),
            status, ltv
        ))
    
    cursor.executemany('''
        INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', customers)
    conn.commit()
    print(f"Generated {count} customers with intentional quality issues")


def generate_products(conn, count=200):
    """Generate product data."""
    cursor = conn.cursor()
    
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports', 'Toys', 'Beauty']
    adjectives = ['Premium', 'Basic', 'Pro', 'Lite', 'Advanced', 'Smart', 'Eco', 'Ultra']
    nouns = ['Widget', 'Gadget', 'Device', 'Tool', 'Kit', 'System', 'Solution', 'Product']
    
    products = []
    for i in range(1, count + 1):
        has_issues = random.random() < 0.1
        
        name = f"{random.choice(adjectives)} {random.choice(nouns)} {i}"
        category = random.choice(categories)
        
        # Price issues: negative, zero, extreme
        if has_issues and random.random() < 0.3:
            price = -random.uniform(10, 100)
        elif has_issues and random.random() < 0.3:
            price = 0
        else:
            price = random.uniform(5, 500)
        
        cost = price * random.uniform(0.3, 0.7) if price > 0 else 0
        
        # Stock issues: negative
        stock = random.randint(-10, 1000) if has_issues else random.randint(0, 1000)
        
        products.append((
            i, name, category, round(price, 2), round(cost, 2), stock,
            random.randint(1, 50),
            datetime.now() - timedelta(days=random.randint(0, 500)),
            random.choice([True, True, True, False])  # 75% active
        ))
    
    cursor.executemany('''
        INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', products)
    conn.commit()
    print(f"Generated {count} products")


def generate_orders(conn, count=5000):
    """Generate order data with referential integrity issues."""
    cursor = conn.cursor()
    
    statuses = ['completed', 'pending', 'shipped', 'cancelled', 'refunded']
    discount_codes = ['SAVE10', 'WELCOME', 'SUMMER20', None, None, None, 'INVALID']
    
    orders = []
    for i in range(1, count + 1):
        has_issues = random.random() < 0.1
        
        # Customer ID issues: non-existent customers
        if has_issues and random.random() < 0.2:
            customer_id = random.randint(10000, 99999)
        else:
            customer_id = random.randint(1, 1000)
        
        # Product ID issues: non-existent products
        if has_issues and random.random() < 0.2:
            product_id = random.randint(10000, 99999)
        else:
            product_id = random.randint(1, 200)
        
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5, 500), 2)
        
        # Total amount calculation issues
        if has_issues and random.random() < 0.3:
            total_amount = quantity * unit_price + random.uniform(-50, 50)  # Wrong calculation
        else:
            total_amount = quantity * unit_price
        
        orders.append((
            i, customer_id, product_id, quantity, unit_price, round(total_amount, 2),
            datetime.now() - timedelta(days=random.randint(0, 365)),
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
            random.choice(statuses),
            random.choice(discount_codes)
        ))
    
    cursor.executemany('''
        INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', orders)
    conn.commit()
    print(f"Generated {count} orders with referential integrity issues")


def generate_sales_transactions(conn, count=10000):
    """Generate high-volume sales transaction data."""
    cursor = conn.cursor()
    
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto', 'cash']
    payment_statuses = ['completed', 'pending', 'failed', 'refunded', 'disputed']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'INVALID']
    
    transactions = []
    for i in range(1, count + 1):
        has_issues = random.random() < 0.08
        
        # Order ID issues: non-existent orders
        if has_issues and random.random() < 0.15:
            order_id = random.randint(100000, 999999)
        else:
            order_id = random.randint(1, 5000)
        
        # Fraud score issues: negative, > 1
        if has_issues and random.random() < 0.2:
            fraud_score = random.uniform(-0.5, 1.5)
        else:
            fraud_score = random.uniform(0, 1)
        
        amount = round(random.uniform(10, 2000), 2)
        
        transactions.append((
            i, order_id,
            random.choice(payment_methods),
            random.choice(payment_statuses),
            amount,
            datetime.now() - timedelta(days=random.randint(0, 365)),
            random.choice(currencies),
            round(fraud_score, 4)
        ))
    
    cursor.executemany('''
        INSERT INTO sales_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', transactions)
    conn.commit()
    print(f"Generated {count} sales transactions")


def create_semi_structured_data():
    """Create semi-structured data (JSON logs, XML configs)."""
    semi_dir = TEST_DATA_DIR / "semi_structured"
    semi_dir.mkdir(exist_ok=True)
    
    # JSON Application Logs
    logs = []
    log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG', 'CRITICAL']
    services = ['api-gateway', 'auth-service', 'payment-processor', 'notification-service', 'data-pipeline']
    
    for i in range(1, 1001):
        has_issues = random.random() < 0.1
        
        log_entry = {
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 10080))).isoformat(),
            "level": random.choice(log_levels),
            "service": random.choice(services),
            "message": f"Operation {random.choice(['completed', 'failed', 'started', 'timeout'])}" if not has_issues else None,
            "user_id": random.randint(1, 1000) if random.random() > 0.1 else "invalid",
            "request_id": f"req-{random.randint(10000, 99999)}",
            "duration_ms": random.randint(10, 5000),
            "metadata": {
                "endpoint": f"/api/v{random.randint(1, 3)}/{random.choice(['users', 'orders', 'products'])}",
                "status_code": random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503]),
                "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
            }
        }
        
        # Introduce schema variations
        if random.random() < 0.05:
            log_entry["extra_field"] = "unexpected"
        if random.random() < 0.05:
            del log_entry["metadata"]
        
        logs.append(log_entry)
    
    with open(semi_dir / "application_logs.json", "w") as f:
        json.dump(logs, f, indent=2)
    
    # JSON User Events
    events = []
    event_types = ['page_view', 'click', 'purchase', 'signup', 'login', 'logout', 'share']
    
    for i in range(1, 2001):
        event = {
            "event_id": f"evt-{i}",
            "event_type": random.choice(event_types),
            "user_id": random.randint(1, 1000),
            "session_id": f"sess-{random.randint(1000, 9999)}",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 10080))).isoformat(),
            "properties": {
                "page_url": f"/products/{random.randint(1, 200)}",
                "referrer": random.choice(['google', 'facebook', 'direct', 'email', None]),
                "device": random.choice(['desktop', 'mobile', 'tablet']),
                "browser": random.choice(['chrome', 'firefox', 'safari', 'edge', '']),
                "utm_source": random.choice(['campaign_a', 'campaign_b', None, None, None]),
            },
            "value": round(random.uniform(0, 500), 2) if random.random() < 0.3 else None
        }
        events.append(event)
    
    with open(semi_dir / "user_events.json", "w") as f:
        json.dump(events, f, indent=2)
    
    # XML Configuration Files
    root = ET.Element("configurations")
    
    for i in range(1, 51):
        config = ET.SubElement(root, "configuration")
        ET.SubElement(config, "id").text = str(i)
        ET.SubElement(config, "name").text = f"Config_{i}"
        ET.SubElement(config, "environment").text = random.choice(['dev', 'staging', 'prod', 'test'])
        ET.SubElement(config, "enabled").text = str(random.choice([True, False]))
        
        settings = ET.SubElement(config, "settings")
        ET.SubElement(settings, "timeout").text = str(random.randint(30, 300))
        ET.SubElement(settings, "retries").text = str(random.randint(0, 5))
        ET.SubElement(settings, "batch_size").text = str(random.choice([100, 500, 1000, 5000]))
        
        features = ET.SubElement(config, "features")
        for feature in ['caching', 'logging', 'metrics', 'alerts']:
            ET.SubElement(features, feature).text = str(random.choice([True, False]))
    
    tree = ET.ElementTree(root)
    tree.write(semi_dir / "system_configs.xml", encoding='utf-8', xml_declaration=True)
    
    print(f"Created semi-structured data in {semi_dir}")


def create_unstructured_data():
    """Create unstructured data with metadata."""
    unstruct_dir = TEST_DATA_DIR / "unstructured"
    unstruct_dir.mkdir(exist_ok=True)
    
    # Customer Support Tickets (text with metadata)
    ticket_categories = ['billing', 'technical', 'account', 'feature_request', 'complaint', 'general']
    priorities = ['low', 'medium', 'high', 'urgent']
    sentiments = ['positive', 'neutral', 'negative', 'very_negative']
    
    tickets = []
    sample_texts = [
        "I'm having trouble accessing my account. The login page keeps showing an error.",
        "Great service! Really appreciate the quick response to my previous inquiry.",
        "This is unacceptable! I've been charged twice for the same order.",
        "Would love to see a dark mode feature in the app. Any plans for this?",
        "The checkout process is confusing. Can you simplify it?",
        "Thank you for resolving my issue so quickly. Very satisfied!",
        "Product arrived damaged. Need a replacement ASAP.",
        "How do I change my subscription plan? Can't find the option.",
    ]
    
    for i in range(1, 501):
        ticket = {
            "ticket_id": f"TICKET-{i:05d}",
            "customer_id": random.randint(1, 1000),
            "category": random.choice(ticket_categories),
            "priority": random.choice(priorities),
            "sentiment": random.choice(sentiments),
            "created_at": (datetime.now() - timedelta(hours=random.randint(0, 720))).isoformat(),
            "resolved_at": (datetime.now() - timedelta(hours=random.randint(0, 48))).isoformat() if random.random() < 0.7 else None,
            "agent_id": f"AGENT-{random.randint(1, 20):03d}" if random.random() < 0.8 else None,
            "subject": f"Support Request #{i}",
            "content": random.choice(sample_texts),
            "attachments": random.randint(0, 3),
            "satisfaction_score": random.randint(1, 5) if random.random() < 0.6 else None
        }
        tickets.append(ticket)
    
    with open(unstruct_dir / "support_tickets.json", "w") as f:
        json.dump(tickets, f, indent=2)
    
    # Product Reviews
    reviews = []
    for i in range(1, 1001):
        review = {
            "review_id": f"REV-{i:06d}",
            "product_id": random.randint(1, 200),
            "customer_id": random.randint(1, 1000),
            "rating": random.randint(1, 5),
            "title": random.choice([
                "Excellent product!", "Not worth the price", "Average quality",
                "Exceeded expectations", "Disappointed", "Would recommend"
            ]),
            "content": random.choice([
                "This product works exactly as described. Very happy with my purchase.",
                "Broke after one week of use. Poor quality materials.",
                "Good value for money. Does what it says on the tin.",
                "Shipping was fast but the packaging was damaged.",
                "Customer service was helpful in resolving my issue.",
            ]),
            "verified_purchase": random.choice([True, True, True, False]),
            "review_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            "helpful_votes": random.randint(0, 100),
            "images_count": random.randint(0, 5)
        }
        reviews.append(review)
    
    with open(unstruct_dir / "product_reviews.json", "w") as f:
        json.dump(reviews, f, indent=2)
    
    print(f"Created unstructured data in {unstruct_dir}")


def create_adls_mock_structure():
    """Create ADLS Gen2 mock file system structure."""
    adls_dir = TEST_DATA_DIR / "adls_mock"
    
    # Create container structure
    containers = ['raw-data', 'processed-data', 'curated-data', 'archive']
    
    for container in containers:
        container_path = adls_dir / container
        container_path.mkdir(parents=True, exist_ok=True)
        
        # Create folder structure
        if container == 'raw-data':
            folders = ['customers', 'orders', 'products', 'transactions', 'logs']
            for folder in folders:
                folder_path = container_path / folder / f"year={datetime.now().year}" / f"month={datetime.now().month:02d}"
                folder_path.mkdir(parents=True, exist_ok=True)
                
                # Create sample files
                for day in range(1, 6):
                    file_path = folder_path / f"data_{day:02d}.csv"
                    with open(file_path, 'w') as f:
                        f.write("id,value,timestamp\n")
                        for i in range(100):
                            f.write(f"{i},{random.uniform(1, 1000)},{datetime.now().isoformat()}\n")
        
        elif container == 'processed-data':
            folders = ['daily-aggregates', 'weekly-reports', 'monthly-summary']
            for folder in folders:
                folder_path = container_path / folder
                folder_path.mkdir(parents=True, exist_ok=True)
                
                # Create parquet-like files (just markers)
                for i in range(1, 4):
                    (folder_path / f"report_{i}.parquet").touch()
        
        elif container == 'curated-data':
            folders = ['analytics', 'ml-features', 'reports']
            for folder in folders:
                (container_path / folder).mkdir(parents=True, exist_ok=True)
    
    # Create a manifest file
    manifest = {
        "storage_account": "testdatalake001",
        "containers": containers,
        "total_files": 150,
        "total_size_gb": 2.5,
        "last_modified": datetime.now().isoformat(),
        "access_tier": "Hot"
    }
    
    with open(adls_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Created ADLS Gen2 mock structure in {adls_dir}")


def create_csv_exports(conn):
    """Export tables to CSV for file-based testing."""
    import csv
    
    struct_dir = TEST_DATA_DIR / "structured"
    struct_dir.mkdir(exist_ok=True)
    
    cursor = conn.cursor()
    
    # Export customers
    cursor.execute("SELECT * FROM customers LIMIT 500")
    customers = cursor.fetchall()
    with open(struct_dir / "customers_sample.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(customers)
    
    # Export orders
    cursor.execute("SELECT * FROM orders LIMIT 1000")
    orders = cursor.fetchall()
    with open(struct_dir / "orders_sample.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(orders)
    
    # Export products
    cursor.execute("SELECT * FROM products")
    products = cursor.fetchall()
    with open(struct_dir / "products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(products)
    
    print(f"Exported CSV files to {struct_dir}")


def generate_statistics(conn):
    """Generate and save database statistics."""
    cursor = conn.cursor()
    
    stats = {
        "generated_at": datetime.now().isoformat(),
        "tables": {}
    }
    
    tables = ['customers', 'orders', 'products', 'sales_transactions']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        
        stats["tables"][table] = {
            "row_count": count,
            "columns": [{"name": col[1], "type": col[2]} for col in columns]
        }
    
    with open(TEST_DATA_DIR / "statistics.json", "w") as f:
        json.dump(stats, f, indent=2)
    
    print(f"Statistics saved to {TEST_DATA_DIR / 'statistics.json'}")


def main():
    """Main setup function."""
    print("=" * 60)
    print("AI Data Quality Agent - Test Database Setup")
    print("=" * 60)
    
    # Remove existing database
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Removed existing database: {DB_PATH}")
    
    # Create database and tables
    conn = sqlite3.connect(DB_PATH)
    print(f"Created database: {DB_PATH}")
    
    # Generate data
    create_structured_tables(conn)
    generate_customers(conn, 1000)
    generate_products(conn, 200)
    generate_orders(conn, 5000)
    generate_sales_transactions(conn, 10000)
    
    # Create other data types
    create_semi_structured_data()
    create_unstructured_data()
    create_adls_mock_structure()
    
    # Export CSVs
    create_csv_exports(conn)
    
    # Generate statistics
    generate_statistics(conn)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("Test database setup complete!")
    print("=" * 60)
    print(f"\nDatabase location: {DB_PATH}")
    print(f"Test data location: {TEST_DATA_DIR}")
    print("\nData types created:")
    print("  - Structured: SQLite tables (customers, orders, products, transactions)")
    print("  - Semi-structured: JSON logs, XML configs")
    print("  - Unstructured: Support tickets, product reviews")
    print("  - ADLS Mock: File system structure")


if __name__ == "__main__":
    main()
