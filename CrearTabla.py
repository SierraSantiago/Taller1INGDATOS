import sqlite3
import csv
from datetime import datetime

# Función para calcular trimestre
def get_quarter(month):
    return (month - 1) // 3 + 1


conn = sqlite3.connect('ventas.db')
cursor = conn.cursor()

# Crear tablas (ejecutar solo una vez)
cursor.executescript('''
CREATE TABLE IF NOT EXISTS DimFecha (
    date_key TEXT PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    weekday TEXT
);

CREATE TABLE IF NOT EXISTS DimCliente (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    customer_city TEXT
);

CREATE TABLE IF NOT EXISTS DimProducto (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL
);

CREATE TABLE IF NOT EXISTS DimRepresentante (
    rep_id TEXT PRIMARY KEY,
    rep_name TEXT,
    region TEXT
);

CREATE TABLE IF NOT EXISTS FactVentas (
    sale_id INTEGER PRIMARY KEY,
    date_key TEXT,
    customer_id TEXT,
    product_id TEXT,
    rep_id TEXT,
    quantity INTEGER,
    FOREIGN KEY (date_key) REFERENCES DimFecha(date_key),
    FOREIGN KEY (customer_id) REFERENCES DimCliente(customer_id),
    FOREIGN KEY (product_id) REFERENCES DimProducto(product_id),
    FOREIGN KEY (rep_id) REFERENCES DimRepresentante(rep_id)
);
''')

with open('sales.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        
        date_obj = datetime.strptime(row['sale_date'], '%Y-%m-%d')
        date_key = row['sale_date']
        year = date_obj.year
        quarter = get_quarter(date_obj.month)
        month = date_obj.month
        day = date_obj.day
        weekday = date_obj.strftime('%A')
        
        
        cursor.execute('''
            INSERT OR IGNORE INTO DimFecha (date_key, year, quarter, month, day, weekday)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (date_key, year, quarter, month, day, weekday))
        
        
        cursor.execute('''
            INSERT OR IGNORE INTO DimCliente (customer_id, customer_name, customer_city)
            VALUES (?, ?, ?)
        ''', (row['customer_id'], row['customer_name'], row['customer_city']))
        
        
        cursor.execute('''
            INSERT OR IGNORE INTO DimProducto (product_id, product_name, category, unit_price)
            VALUES (?, ?, ?, ?)
        ''', (row['product_id'], row['product_name'], row['category'], float(row['unit_price'])))
        
        
        cursor.execute('''
            INSERT OR IGNORE INTO DimRepresentante (rep_id, rep_name, region)
            VALUES (?, ?, ?)
        ''', (row['rep_id'], row['rep_name'], row['region']))
        
        
        cursor.execute('''
            INSERT INTO FactVentas (sale_id, date_key, customer_id, product_id, rep_id, quantity)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (int(row['sale_id']), date_key, row['customer_id'], row['product_id'], row['rep_id'], int(row['quantity'])))

conn.commit()
conn.close()
