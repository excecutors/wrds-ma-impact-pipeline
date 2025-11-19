import pandas as pd
import os

os.makedirs("streamlit_app/dummy_data", exist_ok=True)

data = {
    "deal_id": [101, 102, 103, 104],
    "industry": ["Tech", "Health", "Finance", "Tech"],
    "delta_ev_pct": [0.12, -0.03, 0.05, 0.20],
    "delta_margin_pct": [2.1, -1.0, 0.5, 1.8],
    "deal_size_ratio": [0.3, 0.8, 0.5, 0.2],
}

df = pd.DataFrame(data)
df.to_parquet("streamlit_app/dummy_data/final_results.parquet")

print("Dummy gold file created at streamlit_app/dummy_data/ 🎉")
