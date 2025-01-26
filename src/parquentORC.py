import pandas as pd

# Sample data
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown", "Chris Green"],
    "age": [30, 25, 45, 35, 29],
    "department": ["Engineering", "Marketing", "HR", "Engineering", "Sales"],
    "salary": [70000, 50000, 60000, 80000, 55000],
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save as Parquet
df.to_parquet("data/employee-pq.parquet", index=False, engine="pyarrow")

# Save as ORC
df.to_orc("data/employee-orc.orc", index=False)