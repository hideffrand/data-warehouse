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

    print("Dropping existing tables...")
    cur.execute("""
        DROP TABLE IF EXISTS fact_inventory_balance CASCADE;
        DROP TABLE IF EXISTS fact_inventory_movement CASCADE;
        DROP TABLE IF EXISTS fact_daily_inventory_snapshot CASCADE;
        DROP TABLE IF EXISTS fact_inventory_daily_balance CASCADE;
        DROP TABLE IF EXISTS fact_promotion CASCADE;
        DROP TABLE IF EXISTS dim_promotion CASCADE;
        DROP TABLE IF EXISTS fact_sales CASCADE;

        DROP TABLE IF EXISTS dim_warehouse CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_store CASCADE;
        DROP TABLE IF EXISTS dim_product CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS dim_payment_method CASCADE;
    """)

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
            brand VARCHAR(100),
            cost_per_unit NUMERIC(12,2)   -- HARGA MODAL
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
            discount_amount NUMERIC(12,2),

            -- DERIVED FACT
            gross_profit NUMERIC(12,2),
            margin_percent NUMERIC(12,2)
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

        CREATE TABLE fact_inventory_daily_balance (
            date_key INT REFERENCES dim_date(date_key),
            warehouse_key INT REFERENCES dim_warehouse(warehouse_key),
            product_key INT REFERENCES dim_product(product_key),
            ending_balance INT,
            PRIMARY KEY(date_key, warehouse_key, product_key)
        );
    """)

    print("Seeding dim_date...")
    start = date(2025, 1, 1)
    end = date(2025, 12, 30)

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
    product_rows = []
    for name, category, brand in products:
        # generate cost_per_unit (between 500 and 20.000)
        cost = random.randint(500, 20000)
        product_rows.append((name, category, brand, cost))

    cur.executemany("""
        INSERT INTO dim_product (product_name, category, brand, cost_per_unit)
        VALUES (%s, %s, %s, %s)
    """, product_rows)

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
    # LOAD PRODUCT COSTS FIRST
    # ======================================================
    cur.execute("SELECT product_key, cost_per_unit FROM dim_product")
    cost_map = {pk: float(cost) for (pk, cost) in cur.fetchall()}

    print("Seeding fact_sales...")
    fact_rows = []
    transaction_counter = 1

    # ambil cost per produk
    cur.execute("SELECT product_key, cost_per_unit FROM dim_product")
    cost_map = {pk: float(cost) for (pk, cost) in cur.fetchall()}

    for dkey, full_date, *_ in dates:
        date_str = full_date.strftime(
            "%Y-%m-%d") if hasattr(full_date, "strftime") else str(full_date)

        # jumlah transaksi per hari
        for _ in range(random.randint(100, 120)):
            transaction_id = f"TX{transaction_counter:06d}"
            transaction_counter += 1

            # 1 transaksi bisa punya 1-4 item
            for __ in range(random.randint(1, 4)):
                product_key = random.choice(list(cost_map.keys()))
                store_key = random.randint(1, len(stores))
                customer_key = random.randint(1, len(customers))
                payment_key = random.randint(1, len(payment_methods))

                # cek promo aktif
                active_promos = [
                    i+1 for i, p in enumerate(promotions) if p[3] <= date_str <= p[4]]
                promotion_key = random.choice(
                    active_promos) if active_promos else 1

                quantity = random.randint(1, 120)

                # pastikan unit_price tidak lebih kecil dari cost_per_unit
                unit_price = max(random.randint(1000, 35000),
                                 int(cost_map[product_key]))
                sales_amount = quantity * unit_price
                cost_amount = quantity * cost_map[product_key]

                # discount
                discount = sales_amount * \
                    promotions[promotion_key-1][2] / \
                    100 if promotion_key else random.choice([0, 0, 500])

                # gross profit >= 0
                gross_profit = max(sales_amount - cost_amount, 0)

                # margin_percent >= 0
                margin_percent = (gross_profit / sales_amount *
                                  100) if sales_amount != 0 else 0

                fact_rows.append((
                    dkey, product_key, store_key, customer_key,
                    payment_key, promotion_key, transaction_id,
                    quantity, unit_price, sales_amount, discount,
                    gross_profit, margin_percent
                ))

    # insert ke DB
    cur.executemany("""
        INSERT INTO fact_sales 
        (date_key, product_key, store_key, customer_key, 
        payment_method_key, promotion_key, transaction_id,
        quantity, unit_price, sales_amount, discount_amount,
        gross_profit, margin_percent)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

    snapshot_start = date(2025, 1, 1)
    snapshot_end = date(2025, 12, 30)

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

    # Asumsi koneksi cur tersedia
    snapshot_start = date(2025, 1, 1)
    snapshot_end = date(2025, 12, 30)

    current = snapshot_start
    while current <= snapshot_end:
        date_key = int(current.strftime("%Y%m%d"))
        for warehouse_key in range(1, 5):  # misal 4 warehouse
            for product_key in range(1, 21):  # misal 20 produk
                ending_balance = random.randint(50, 500)
                cur.execute("""
                    INSERT INTO fact_inventory_daily_balance
                    (date_key, warehouse_key, product_key, ending_balance)
                    VALUES (%s, %s, %s, %s)
                """, (date_key, warehouse_key, product_key, ending_balance))
        current += timedelta(days=1)

    print("SEED DONE!")
    conn.commit()
    conn.close()
    print("Database initialized successfully!")


if __name__ == "__main__":
    init_database()
