import random
from datetime import date, timedelta
import psycopg2

DB_CONFIG = {
    "dbname": "retail_dw",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": 5432,
}


def init_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ======================================================
    # DROP TABLES (drop in order to avoid FK issues)
    # ======================================================
    print("Dropping existing tables...")
    cur.execute("""
        DROP TABLE IF EXISTS fact_inventory_balance CASCADE;
        DROP TABLE IF EXISTS fact_inventory_movement CASCADE;
        DROP TABLE IF EXISTS fact_daily_inventory_snapshot CASCADE;

        DROP TABLE IF EXISTS fact_promotion CASCADE;
        DROP TABLE IF EXISTS dim_promotion CASCADE;

        DROP TABLE IF EXISTS fact_sales CASCADE;
        DROP TABLE IF EXISTS fact_product_avg_price CASCADE;
        DROP TABLE IF EXISTS fact_product_frequency CASCADE;
        DROP TABLE IF EXISTS fact_product_store CASCADE;
        DROP TABLE IF EXISTS fact_product_daily CASCADE;
        DROP TABLE IF EXISTS fact_product_sales CASCADE;

        DROP TABLE IF EXISTS dim_warehouse CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_store CASCADE;
        DROP TABLE IF EXISTS dim_product CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS dim_payment_method CASCADE;
    """)

    # ======================================================
    # CREATE DIMENSIONS
    # ======================================================
    print("Creating dimension tables...")
    cur.execute("""
        CREATE TABLE dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            day_name VARCHAR(10),
            month_name VARCHAR(10)
        );

        CREATE TABLE dim_store (
            store_key SERIAL PRIMARY KEY,
            store_name VARCHAR(100),
            city VARCHAR(100),
            region VARCHAR(100)
        );

        CREATE TABLE dim_product (
            product_key SERIAL PRIMARY KEY,
            product_name VARCHAR(200),
            category VARCHAR(100),
            brand VARCHAR(100)
        );

        CREATE TABLE dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_name VARCHAR(100),
            gender VARCHAR(20),
            age INT
        );

        CREATE TABLE dim_payment_method (
            payment_method_key SERIAL PRIMARY KEY,
            payment_type VARCHAR(50)
        );

        CREATE TABLE dim_promotion (
            promotion_key SERIAL PRIMARY KEY,
            promotion_name VARCHAR(200),
            promotion_type VARCHAR(50),
            discount_percent INT,
            start_date DATE,
            end_date DATE
        );

        CREATE TABLE dim_warehouse (
            warehouse_key SERIAL PRIMARY KEY,
            warehouse_name VARCHAR(200),
            city VARCHAR(100),
            region VARCHAR(100),
            capacity INT
        );
    """)

    # ======================================================
    # CREATE FACTS
    # ======================================================
    print("Creating fact table(s)...")
    cur.execute("""
        CREATE TABLE fact_sales (
            sales_key SERIAL PRIMARY KEY,
            date_key INT REFERENCES dim_date(date_key),
            product_key INT REFERENCES dim_product(product_key),
            store_key INT REFERENCES dim_store(store_key),
            customer_key INT REFERENCES dim_customer(customer_key),
            payment_method_key INT REFERENCES dim_payment_method(payment_method_key),
            promotion_key INT REFERENCES dim_promotion(promotion_key),
            transaction_id VARCHAR(50),
            quantity INT,
            unit_price NUMERIC(12,2),
            sales_amount NUMERIC(12,2),
            discount_amount NUMERIC(12,2)
        );

        CREATE TABLE fact_promotion (
            promotion_key INT REFERENCES dim_promotion(promotion_key),
            date_key INT REFERENCES dim_date(date_key),
            store_key INT REFERENCES dim_store(store_key)
        );

        -- Snapshot fact: daily inventory snapshot per warehouse/product/date
        CREATE TABLE fact_daily_inventory_snapshot (
            snapshot_key SERIAL PRIMARY KEY,
            date_key INT REFERENCES dim_date(date_key),
            warehouse_key INT REFERENCES dim_warehouse(warehouse_key),
            product_key INT REFERENCES dim_product(product_key),
            on_hand_qty INT,
            reserved_qty INT,
            inbound_qty INT
        );

        -- Accumulation fact: inventory movement events
        CREATE TABLE fact_inventory_movement (
            movement_key SERIAL PRIMARY KEY,
            movement_type VARCHAR(50),   -- IN, OUT, TRANSFER_IN, TRANSFER_OUT, ADJUSTMENT
            date_key INT REFERENCES dim_date(date_key),
            warehouse_key INT REFERENCES dim_warehouse(warehouse_key),
            product_key INT REFERENCES dim_product(product_key),
            quantity INT,
            remarks TEXT
        );

        -- Semi-additive fact: current balance per warehouse/product (snapshot of latest)
        CREATE TABLE fact_inventory_balance (
            warehouse_key INT REFERENCES dim_warehouse(warehouse_key),
            product_key INT REFERENCES dim_product(product_key),
            ending_balance INT,
            last_updated TIMESTAMP,
            PRIMARY KEY (warehouse_key, product_key)
        );
    """)

    # ======================================================
    # SEED DIM DATE
    # ======================================================
    print("Seeding dim_date...")
    start = date(2025, 10, 1)
    end = date(2025, 11, 30)

    dates = []
    current = start
    while current <= end:
        dk = int(current.strftime("%Y%m%d"))
        dates.append((dk, current, current.year, current.month, current.day,
                      current.strftime("%a"), current.strftime("%b")))
        current += timedelta(days=1)

    cur.executemany("""
        INSERT INTO dim_date
        (date_key, full_date, year, month, day, day_name, month_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, dates)

    # ======================================================
    # SEED DIM STORE
    # ======================================================
    print("Seeding dim_store...")
    stores = [
        ("Indomaret A", "Jakarta", "Jabodetabek"),
        ("Indomaret B", "Bandung", "Jawa Barat"),
        ("Indomaret C", "Surabaya", "Jawa Timur"),
        ("Indomaret Point D", "Jakarta", "Jabodetabek"),
        ("Indomaret Fresh E", "Depok", "Jabodetabek"),
        ("Indomaret F", "Medan", "Sumut"),
        ("Indomaret G", "Makassar", "Sulsel"),
        ("Indomaret H", "Surabaya", "Jawa Timur"),
        ("Indomaret Point I", "Bandung", "Jawa Barat"),
        ("Indomaret J", "Bali", "Bali"),
    ]

    cur.executemany("""
        INSERT INTO dim_store (store_name, city, region)
        VALUES (%s, %s, %s)
    """, stores)

    # ======================================================
    # SEED DIM PRODUCT
    # ======================================================
    print("Seeding dim_product...")
    products = [
        ("Aqua 600ml", "Minuman", "Aqua"),
        ("Aqua 1500ml", "Minuman", "Aqua"),
        ("Indomie Goreng", "Makanan", "Indofood"),
        ("Indomie Kari Ayam", "Makanan", "Indofood"),
        ("Chitato 185g", "Snack", "Chitato"),
        ("Lays BBQ", "Snack", "Lays"),
        ("Sprite 390ml", "Minuman", "Coca Cola"),
        ("Coca Cola 390ml", "Minuman", "Coca Cola"),
        ("Teh Pucuk Harum", "Minuman", "Mayora"),
        ("Kopi Kapal Api", "Minuman", "Kapal Api"),
        ("Roma Kelapa", "Snack", "Roma"),
        ("Tango Wafer", "Snack", "Tango"),
        ("Silverqueen Mini", "Snack", "Silverqueen"),
        ("Good Day Freeze", "Minuman", "Good Day"),
        ("Mizone 500ml", "Minuman", "Mizone"),
        ("Ultramilk Coklat", "Minuman", "Ultramilk"),
        ("Bear Brand", "Minuman", "Nestle"),
        ("Yakult", "Minuman", "Yakult"),
        ("Pepsodent 190g", "Personal Care", "Pepsodent"),
        ("Sunsilk Hitam", "Personal Care", "Sunsilk"),
    ]
    cur.executemany("""
        INSERT INTO dim_product (product_name, category, brand)
        VALUES (%s, %s, %s)
    """, products)

    # ======================================================
    # SEED DIM CUSTOMER
    # ======================================================
    print("Seeding dim_customer...")
    customers = []
    male_names = ["Budi", "Agus", "Doni", "Rangga",
                  "Rudi", "Kevin", "Andre", "Rizky"]
    female_names = ["Siti", "Anisa", "Nadia",
                    "Putri", "Dewi", "Ratna", "Ayu", "Bella"]

    for i in range(50):
        if random.random() < 0.5:
            name = random.choice(male_names)
            gender = "M"
        else:
            name = random.choice(female_names)
            gender = "F"

        age = random.randint(18, 55)
        customers.append((f"Customer {name}{i}", gender, age))

    cur.executemany("""
        INSERT INTO dim_customer (customer_name, gender, age)
        VALUES (%s, %s, %s)
    """, customers)

    # ======================================================
    # SEED PAYMENT METHODS
    # ======================================================
    print("Seeding dim_payment_method...")
    payment_methods = [
        ("CASH",),
        ("OVO",),
        ("GOPAY",),
        ("DANA",),
        ("EDC",),
    ]
    cur.executemany("""
        INSERT INTO dim_payment_method (payment_type)
        VALUES (%s)
    """, payment_methods)

    # ======================================================
    # SEED DIM PROMOTION
    # ======================================================
    print("Seeding dim_promotion...")
    promotions = [
        ("Diskon 10% Semua Minuman", "Discount", 10, "2025-10-01", "2025-10-15"),
        ("Diskon 5% Semua Snack", "Discount", 5, "2025-10-10", "2025-10-20"),
        ("Promo Member 15%", "Member", 15, "2025-10-21", "2025-10-31"),
        ("Cashback 10%",
         "Cashback", 10, "2025-11-01", "2025-11-30")
    ]

    cur.executemany("""
        INSERT INTO dim_promotion (promotion_name, promotion_type, discount_percent, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
    """, promotions)

    # ======================================================
    # SEED FACTLESS FACT PROMOTION
    # ======================================================
    print("Seeding fact_promotion...")
    fact_promo_rows = []
    for promo_id, (_, _, _, start, end) in enumerate(promotions, start=1):
        for dkey, full_date, *_ in dates:
            if start <= str(full_date) <= end:
                # promo berlaku di semua store untuk sederhana
                for store_key in range(1, len(stores) + 1):
                    fact_promo_rows.append((promo_id, dkey, store_key))

    cur.executemany("""
        INSERT INTO fact_promotion (promotion_key, date_key, store_key)
        VALUES (%s, %s, %s)
    """, fact_promo_rows)

    # ======================================================
    # SEED FACT SALES
    # ======================================================
    print("Seeding fact_sales...")
    fact_rows = []
    transaction_counter = 1

    for dkey, full_date, *_ in dates:
        for _ in range(random.randint(100, 120)):
            transaction_id = f"TX{transaction_counter:04d}"
            transaction_counter += 1

            for __ in range(random.randint(1, 4)):
                product_key = random.randint(1, len(products))
                store_key = random.randint(1, len(stores))
                customer_key = random.randint(1, len(customers))
                payment_key = random.randint(1, len(payment_methods))

                # determine promo if exists
                active_promos = [
                    p_id for (p_id, _, _, _, start, end) in [
                        (i+1, *promotions[i]) for i in range(len(promotions))
                    ]
                    if promotions[p_id-1][3] <= str(full_date) <= promotions[p_id-1][4]
                ]
                promotion_key = random.choice(
                    active_promos) if active_promos else None

                quantity = random.randint(1, 120)
                unit_price = random.randint(1000, 35000)
                amount = quantity * unit_price

                discount_pct = promotions[promotion_key -
                                          1][2] if promotion_key else 0
                discount = (amount * discount_pct /
                            100) if promotion_key else random.choice([0, 0, 500])

                fact_rows.append((
                    dkey, product_key, store_key, customer_key,
                    payment_key, promotion_key, transaction_id,
                    quantity, unit_price, amount, discount
                ))

    cur.executemany("""
        INSERT INTO fact_sales 
        (date_key, product_key, store_key, customer_key, payment_method_key,
         promotion_key, transaction_id, quantity, unit_price, sales_amount, discount_amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, fact_rows)

    print("Seeding dim_warehouse...")
    cur.execute("""
        INSERT INTO dim_warehouse (warehouse_name, city, region, capacity)
        VALUES 
        ('Gudang Pusat Jakarta', 'Jakarta', 'Jawa Barat', 50000),
        ('Gudang Surabaya', 'Surabaya', 'Jawa Timur', 45000),
        ('Gudang Bandung', 'Bandung', 'Jawa Barat', 30000),
        ('Gudang Medan', 'Medan', 'Sumatera Utara', 25000)
    """)

    print("Seeding fact_daily_inventory_snapshot...")
    snapshot_records = []

    snapshot_start = date(2025, 10, 1)
    snapshot_end = date(2025, 11, 30)

    current = snapshot_start

    while current <= snapshot_end:
        date_key = int(current.strftime("%Y%m%d"))

        for product_key in range(1, 21):  # 20 produk
            warehouse_key = random.randint(1, 4)

            on_hand = random.randint(50, 300)
            reserved = random.randint(0, 20)
            inbound = random.randint(0, 50)

            snapshot_records.append(
                (date_key, warehouse_key, product_key, on_hand, reserved, inbound))

        current += timedelta(days=1)

    cur.executemany("""
        INSERT INTO fact_daily_inventory_snapshot
        (date_key, warehouse_key, product_key, on_hand_qty, reserved_qty, inbound_qty)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, snapshot_records)

    print("Seeding fact_inventory_movement...")
    movement_types = ["IN", "OUT", "TRANSFER_IN", "TRANSFER_OUT", "ADJUSTMENT"]
    for _ in range(800):  # 800 movement events
        dt = snapshot_start + \
            timedelta(days=random.randint(
                0, (snapshot_end - snapshot_start).days))
        date_key = int(dt.strftime("%Y%m%d"))

        cur.execute("""
            INSERT INTO fact_inventory_movement
                (movement_type, date_key, warehouse_key, product_key, quantity, remarks)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            random.choice(movement_types),
            date_key,
            random.randint(1, 4),   # warehouse
            random.randint(1, 20),  # product
            random.randint(1, 200),
            "auto-generated"
        ))

    print("Seeding fact_inventory_balance...")
    last_date_key = int(snapshot_end.strftime("%Y%m%d"))
    cur.execute("""
        SELECT warehouse_key, product_key, on_hand_qty
        FROM fact_daily_inventory_snapshot
        WHERE date_key = %s
    """, (last_date_key,))

    rows = cur.fetchall()

    for w_key, p_key, balance in rows:
        cur.execute("""
            INSERT INTO fact_inventory_balance
                (warehouse_key, product_key, ending_balance, last_updated)
            VALUES (%s, %s, %s, NOW())
        """, (w_key, p_key, balance))

    print("SEED DONE!")

    # ======================================================
    # CREATE VIEWS
    # ======================================================
    print("Creating views...")
    cur.execute("""
        CREATE OR REPLACE VIEW vw_daily_sales AS
        SELECT
            f.date_key,
            d.full_date,
            SUM(f.sales_amount) AS total_sales,
            SUM(f.quantity) AS total_qty
        FROM fact_sales f
        JOIN dim_date d ON d.date_key = f.date_key
        GROUP BY f.date_key, d.full_date
        ORDER BY d.full_date;

        CREATE OR REPLACE VIEW vw_payment_summary AS
        SELECT
            pm.payment_type,
            SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_payment_method pm
            ON pm.payment_method_key = f.payment_method_key
        GROUP BY pm.payment_type
        ORDER BY total_sales DESC;

        CREATE OR REPLACE VIEW vw_top_products AS
        SELECT 
            p.product_name,
            SUM(f.sales_amount) AS total_sales,
            SUM(f.quantity) AS total_qty
        FROM fact_sales f
        JOIN dim_product p ON p.product_key = f.product_key
        GROUP BY p.product_name
        ORDER BY total_sales DESC;

        CREATE OR REPLACE VIEW fact_product_sales AS
        SELECT 
            p.product_key,
            p.product_name,
            p.category,
            p.brand,
            SUM(f.quantity) AS total_quantity_sold,
            SUM(f.sales_amount) AS total_sales_amount,
            SUM(f.discount_amount) AS total_discount,
            COUNT(DISTINCT f.transaction_id) AS total_transactions
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_key, p.product_name, p.category, p.brand
        ORDER BY total_sales_amount DESC;

        CREATE OR REPLACE VIEW fact_product_daily AS
        SELECT 
            f.date_key,
            d.full_date,
            f.product_key,
            p.product_name,
            SUM(f.quantity) AS qty_sold,
            SUM(f.sales_amount) AS sales_amount
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY f.date_key, d.full_date, f.product_key, p.product_name
        ORDER BY full_date, product_key;

        CREATE OR REPLACE VIEW fact_product_store AS
        SELECT
            s.store_key,
            s.store_name,
            f.product_key,
            p.product_name,
            SUM(f.quantity) AS total_qty,
            SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_store s ON f.store_key = s.store_key
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY s.store_key, s.store_name, f.product_key, p.product_name
        ORDER BY s.store_key, total_sales DESC;

        CREATE OR REPLACE VIEW fact_product_frequency AS
        SELECT
            p.product_key,
            p.product_name,
            COUNT(DISTINCT f.transaction_id) AS basket_count,
            SUM(f.quantity) AS total_units
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_key, p.product_name
        ORDER BY basket_count DESC;

        CREATE OR REPLACE VIEW fact_product_avg_price AS
        SELECT
            p.product_key,
            p.product_name,
            AVG(f.unit_price) AS avg_price,
            MIN(f.unit_price) AS min_price,
            MAX(f.unit_price) AS max_price
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.product_key, p.product_name
        ORDER BY avg_price DESC;
    """)

    conn.commit()
    conn.close()
    print("Database initialized successfully!")


if __name__ == "__main__":
    init_database()
