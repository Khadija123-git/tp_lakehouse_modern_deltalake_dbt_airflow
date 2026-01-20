import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import os

# Configuration
np.random.seed(42)
random.seed(42)

RAW_DATA_PATH = os.path.expanduser("~/lakehouse/raw_data")
os.makedirs(RAW_DATA_PATH, exist_ok=True)

NUM_CUSTOMERS = 5000
NUM_PRODUCTS = 500
NUM_TRANSACTIONS = 50000

print("=" * 60)
print("GÉNÉRATION DES DONNÉES DE TEST - TECHMART")
print("=" * 60)

# Génération des clients
print("\n[1/3] Génération des clients...")
prenoms = ['Ahmed', 'Fatima', 'Mohamed', 'Amina', 'Youssef', 'Khadija', 
           'Omar', 'Zineb', 'Hassan', 'Laila']
noms = ['Bennani', 'El Alaoui', 'Benjelloun', 'Tazi', 'Fassi', 
        'Kettani', 'Berrada', 'Chraibi']
villes = ['Casablanca', 'Rabat', 'Fes', 'Marrakech', 'Tanger', 'Agadir']

customers_data = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer = {
        'customer_id': i,
        'first_name': random.choice(prenoms),
        'last_name': random.choice(noms),
        'email': f'customer{i}@email.com',
        'phone': f'+212{random.randint(600000000, 799999999)}',
        'city': random.choice(villes),
        'country': random.choice(['Maroc'] * 70 + ['France'] * 20 + ['Belgique'] * 10),
        'registration_date': (datetime(2020, 1, 1) + 
                            timedelta(days=random.randint(0, 1400))).strftime('%Y-%m-%d'),
        'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
        'is_active': random.choice([True] * 9 + [False])
    }
    customers_data.append(customer)

df_customers = pd.DataFrame(customers_data)
df_customers.to_csv(f'{RAW_DATA_PATH}/customers.csv', index=False)
print(f"  Clients générés : {len(df_customers):,}")

# Génération des produits
print("\n[2/3] Génération des produits...")
categories = {
    'Electronique': ['Smartphone', 'Tablette', 'Laptop', 'Écouteurs'],
    'Mode': ['T-shirt', 'Pantalon', 'Chaussures', 'Sac'],
    'Maison': ['Canapé', 'Table', 'Lampe', 'Tapis'],
    'Sport': ['Ballon', 'Vélo', 'Tapis yoga', 'Haltères']
}

marques = ['Samsung', 'Apple', 'Nike', 'Adidas', 'IKEA', 'Zara']

products_data = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(list(categories.keys()))
    product = {
        'product_id': i,
        'product_name': f'{random.choice(categories[category])} Pro {random.randint(100, 999)}',
        'category': category,
        'sub_category': random.choice(categories[category]),
        'brand': random.choice(marques),
        'price': round(random.uniform(50, 5000), 2),
        'cost': round(random.uniform(30, 3000), 2),
        'stock_quantity': random.randint(0, 1000),
        'is_available': random.choice([True] * 8 + [False] * 2)
    }
    products_data.append(product)

df_products = pd.DataFrame(products_data)
df_products.to_csv(f'{RAW_DATA_PATH}/products.csv', index=False)
print(f"  Produits générés : {len(df_products):,}")

# Génération des transactions
print("\n[3/3] Génération des transactions...")
transactions_data = []
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)

for i in range(1, NUM_TRANSACTIONS + 1):
    transaction_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    product_id = random.randint(1, NUM_PRODUCTS)
    product = df_products[df_products['product_id'] == product_id].iloc[0]
    
    quantity = random.randint(1, 5)
    unit_price = product['price']
    discount_rate = random.choice([0, 0, 0, 0.05, 0.10, 0.15])
    
    subtotal = unit_price * quantity
    discount_amount = subtotal * discount_rate
    tax_amount = (subtotal - discount_amount) * 0.20
    total_amount = subtotal - discount_amount + tax_amount
    
    transaction = {
        'transaction_id': i,
        'customer_id': random.randint(1, NUM_CUSTOMERS),
        'product_id': product_id,
        'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
        'quantity': quantity,
        'unit_price': unit_price,
        'discount_rate': discount_rate,
        'discount_amount': round(discount_amount, 2),
        'tax_amount': round(tax_amount, 2),
        'total_amount': round(total_amount, 2),
        'payment_method': random.choice(['Carte', 'PayPal', 'Virement']),
        'status': random.choice(['Completed'] * 8 + ['Cancelled', 'Refunded']),
        'is_fraudulent': random.choice([False] * 99 + [True])
    }
    transactions_data.append(transaction)

df_transactions = pd.DataFrame(transactions_data)
df_transactions.to_csv(f'{RAW_DATA_PATH}/transactions.csv', index=False)
print(f"  Transactions générées : {len(df_transactions):,}")

# Statistiques finales
print("\n" + "=" * 60)
print("GÉNÉRATION TERMINÉE")
print("=" * 60)
print(f"Clients          : {len(df_customers):,}")
print(f"Produits         : {len(df_products):,}")
print(f"Transactions     : {len(df_transactions):,}")
print(f"\nCA Total         : {df_transactions['total_amount'].sum():,.2f} MAD")
print(f"Panier moyen     : {df_transactions['total_amount'].mean():,.2f} MAD")
print(f"\nFichiers dans : {RAW_DATA_PATH}")
print("=" * 60)