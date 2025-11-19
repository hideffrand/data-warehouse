import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "dbname": "retail_dw",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": 5432,
}

def seed():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ----- DIM DATE -----
    print("📌 Seeding dim_date...")
    base_date = datetime(2025, 11, 1)
    for i in range(10):
        d = base_date + timedelta(days=i)
        date_key = int(d.strftime("%Y%m%d"))
        cur.execute("""
            INSERT INTO dim_date (date_key, full_date, year, month, day, weekday)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING;
        """, (date_key, d, d.year, d.month, d.day, d.strftime("%A")))

    # ----- DIM PRODUCT -----
    print("📌 Seeding dim_product...")
    cur.execute("""
        INSERT INTO dim_product (product_name, category, brand)
        VALUES 
        ('Aqua', 'Beverage', 'Danone'),
        ('Indomie Ayam Bawang', 'Food', 'Indofood'),
        ('Kopi Kapal Api', 'Beverage', 'Kapal Api')
        RETURNING product_key;
    """)

    # ----- DIM STORE -----
    print("📌 Seeding dim_store...")
    cur.execute("""
        INSERT INTO dim_store (store_name, city, region)
        VALUES
        ('Indomaret A', 'Jakarta', 'JKT'),
        ('Indomaret B', 'Bandung', 'BDG'),
        ('Indomaret C', 'Surabaya', 'SBY');
    """)

    # ----- DIM CUSTOMER -----
    print("📌 Seeding dim_customer...")
    cur.execute("""
        INSERT INTO dim_customer (customer_name, gender, age)
        VALUES
        ('Andi', 'M', 25),
        ('Budi', 'M', 30),
        ('Sari', 'F', 28);
    """)

    # ----- DIM PAYMENT -----
    print("📌 Seeding dim_payment_method...")
    cur.execute("""
        INSERT INTO dim_payment_method (payment_type)
        VALUES ('CASH'), ('OVO'), ('EDC');
    """)

    # ----- FACT SALES -----
    print("📌 Seeding fact_sales...")
    cur.execute("""
        INSERT INTO fact_sales (
            date_key, product_key, store_key, customer_key, payment_key,
            transaction_id, quantity, unit_price, gross_amount, discount_amount, net_amount
        )
        VALUES
        (20251101, 1, 1, 1, 1, 'TRX001', 2, 3500, 7000, 0, 7000),
        (20251101, 2, 1, 2, 2, 'TRX001', 1, 5000, 5000, 0, 5000),
        (20251102, 3, 2, 3, 3, 'TRX002', 1, 12000, 12000, 0, 12000),
        (20251102, 1, 2, 1, 1, 'TRX002', 2, 15000, 30000, 0, 30000),
        (20251103, 2, 3, 2, 2, 'TRX003', 1, 17000, 17000, 0, 17000),
        (20251103, 3, 3, 3, 1, 'TRX003', 1, 18000, 18000, 0, 18000),
        (20251104, 1, 1, 1, 1, 'TRX004', 2, 5000, 10000, 0, 10000),
        (20251104, 3, 2, 2, 3, 'TRX005', 1, 19000, 19000, 0, 19000),
        (20251105, 2, 3, 3, 2, 'TRX006', 1, 10000, 10000, 0, 10000),
        (20251105, 3, 3, 1, 1, 'TRX006', 2, 16000, 32000, 0, 32000);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Seeding completed successfully!")

if __name__ == "__main__":
    seed()
