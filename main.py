import pandas as pd

# Load the dataset CSV into a DataFrame
df = pd.read_excel(
    "/Users/jack/Desktop/project/cursor/ce306_python/mock survey data.xlsx",
    engine='openpyxl'  # Explicitly specify engine if needed
)
# Select the first 1000 rows
sample_df = df.head(1000)
print("Number of documents in sample:", len(sample_df))

# Save the sample DataFrame to a new CSV file
sample_df.to_csv("sample_data.csv", index=False)

print("Sample data saved to sample_data.csv")
