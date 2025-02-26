import pandas as pd
import random
from datetime import datetime, timedelta

# Generate synthetic macroeconomic indicators data
start_date = datetime(2023, 1, 1)
num_months = 24  # Generate 2 years of data

data = []
for i in range(num_months):
    date = (start_date + timedelta(days=i * 30)).strftime("%Y-%m-%d")
    cash_rate = round(random.uniform(3.0, 5.5), 2)
    unemployment_rate = round(random.uniform(4.0, 6.5), 1)
    cpi = round(100 + i * 0.5 + random.uniform(-1, 1), 1)
    gdp_growth = round(random.uniform(1.5, 3.5), 2)
    inflation_rate = round(random.uniform(2.0, 4.5), 2)
    exchange_rate = round(random.uniform(1.3, 1.6), 2)

    data.append([date, cash_rate, unemployment_rate, cpi, gdp_growth, inflation_rate, exchange_rate])

# Create DataFrame
df_macro = pd.DataFrame(data, columns=[
    "Date", "Cash_Rate (%)", "Unemployment_Rate (%)", "CPI", "GDP_Growth (%)", "Inflation_Rate (%)", "Exchange_Rate (USD/AUD)"
])

# Save as CSV
macro_file_path = "../data/synthetic_macro_data.csv"
df_macro.to_csv(macro_file_path, index=False)
