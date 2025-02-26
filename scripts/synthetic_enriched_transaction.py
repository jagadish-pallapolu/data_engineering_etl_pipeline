import pandas as pd
import random
from datetime import datetime, timedelta

# Generate synthetic enriched transaction data
num_records = 1000
locations = ["Sydney, NSW", "Melbourne, VIC", "Brisbane, QLD", "Perth, WA", "Adelaide, SA"]
merchant_types = ["Retail Grocery", "Electronics", "Dining", "E-Commerce", "Fashion", "Fuel Station"]
risk_scores = list(range(1, 11))  # Risk score from 1 to 10
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 2, 20)
merchants = ["Supermarket", "Electronics", "Clothing", "Restaurant", "Online Store"]
payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"]

# Add enrichment details to the transaction dataset
enriched_data = []
for i in range(num_records):
    transaction_id = f"TXN{random.randint(100000, 999999)}"
    transaction_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    amount = round(random.uniform(5.0, 500.0), 2)
    merchant = random.choice(merchants)
    payment_method = random.choice(payment_methods)
    terminal_id = f"TERM{random.randint(100, 999)}"
    location = random.choice(locations)
    merchant_type = random.choice(merchant_types)
    risk_score = random.choice(risk_scores)

    enriched_data.append(
        [transaction_id, transaction_date.strftime("%Y-%m-%d"), amount, merchant, payment_method, terminal_id, location,
         merchant_type, risk_score])

# Create DataFrame
df_enriched = pd.DataFrame(enriched_data, columns=[
    "Transaction_ID", "Transaction_Date", "Amount (AUD)", "Merchant", "Payment_Method", "Terminal_ID", "Location",
    "Merchant_Type", "Risk_Score (1-10)"
])

# Save as CSV
enriched_file_path = "../data/synthetic_enriched_transactions.csv"
df_enriched.to_csv(enriched_file_path, index=False)
