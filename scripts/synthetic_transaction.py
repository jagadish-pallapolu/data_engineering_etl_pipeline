import pandas as pd
import random
from datetime import datetime, timedelta

# Generate synthetic transaction data
num_records = 1000
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 2, 20)
merchants = ["Supermarket", "Electronics", "Clothing", "Restaurant", "Online Store"]
payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"]

data = []
for _ in range(num_records):
    transaction_id = f"TXN{random.randint(100000, 999999)}"
    transaction_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    amount = round(random.uniform(5.0, 500.0), 2)
    merchant = random.choice(merchants)
    payment_method = random.choice(payment_methods)

    data.append([transaction_id, transaction_date.strftime("%Y-%m-%d"), amount, merchant, payment_method])

# Create DataFrame
df = pd.DataFrame(data, columns=["Transaction_ID", "Transaction_Date", "Amount", "Merchant", "Payment_Method"])

# Save as CSV
file_path = "../data/synthetic_transactions.csv"
df.to_csv(file_path, index=False)
