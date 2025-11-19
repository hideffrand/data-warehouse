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
        DROP TABLE IF EXISTS fact_sales CASCADE;
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
    """)

    print("Creating fact table...")
    cur.execute("""
        CREATE TABLE fact_sales (
            sales_key SERIAL PRIMARY KEY,
            date_key INT REFERENCES dim_date(date_key),
            product_key INT REFERENCES dim_product(product_key),
            store_key INT REFERENCES dim_store(store_key),
            customer_key INT REFERENCES dim_customer(customer_key),
            payment_method_key INT REFERENCES dim_payment_method(payment_method_key),

            transaction_id VARCHAR(50),
            quantity INT,
            unit_price NUMERIC(12,2),
            sales_amount NUMERIC(12,2),
            discount_amount NUMERIC(12,2)
        );
    """)

    # ============================
    # SEED DIMENSIONS
    # ============================

    print("Seeding dim_date...")
    # Generate tanggal otomatis
    from datetime import date, timedelta

    start = date(2025, 11, 1)
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
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, dates)


    print("Seeding dim_store...")
    stores = [
        ("Indomaret A", "Jakarta", "Jabodetabek"),
        ("Indomaret B", "Bandung", "Jawa Barat"),
        ("Indomaret C", "Surabaya", "Jawa Timur"),
        ("Alfamart A", "Jakarta", "Jabodetabek"),
        ("Alfamart B", "Depok", "Jabodetabek"),
        ("Indomaret D", "Medan", "Sumut"),
        ("Indomaret E", "Makassar", "Sulsel"),
        ("Alfamart C", "Surabaya", "Jawa Timur"),
        ("Alfamart D", "Bandung", "Jawa Barat"),
        ("Indomaret F", "Bali", "Bali"),
    ]

    cur.executemany("""
        INSERT INTO dim_store (store_name, city, region)
        VALUES (%s, %s, %s);
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

    cur.executemany("""
        INSERT INTO dim_product (product_name, category, brand)
        VALUES (%s, %s, %s);
    """, products)


    print("Seeding dim_customer...")
    import random

    customers = []
    male_names = ["Budi", "Agus", "Doni", "Rangga", "Rudi", "Kevin", "Andre", "Rizky"]
    female_names = ["Siti", "Anisa", "Nadia", "Putri", "Dewi", "Ratna", "Ayu", "Bella"]

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
        VALUES (%s, %s, %s);
    """, customers)


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
        VALUES (%s);
    """, payment_methods)


    # ============================
    # SEED FACT SALES — 200+ transaksi
    # ============================

    print("Seeding fact_sales...")

    fact_rows = []
    transaction_counter = 1

    for dkey, full_date, *_ in dates:

        # setiap tanggal ada 5–15 transaksi
        for _ in range(random.randint(5, 15)):

            transaction_id = f"TX{transaction_counter:04d}"
            transaction_counter += 1

            # setiap transaksi jual 1–4 produk
            for __ in range(random.randint(1, 4)):

                product_key = random.randint(1, len(products))
                store_key = random.randint(1, len(stores))
                customer_key = random.randint(1, len(customers))
                payment_key = random.randint(1, len(payment_methods))

                quantity = random.randint(1, 5)
                unit_price = random.randint(3000, 25000)
                amount = quantity * unit_price
                discount = random.choice([0, 0, 0, 500, 1000])  # kadang diskon

                fact_rows.append((
                    dkey, product_key, store_key, customer_key,
                    payment_key, transaction_id,
                    quantity, unit_price, amount, discount
                ))

    cur.executemany("""
        INSERT INTO fact_sales 
        (date_key, product_key, store_key, customer_key, payment_method_key,
        transaction_id, quantity, unit_price, sales_amount, discount_amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """, fact_rows)

    print("SEED DONE!")


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
