import duckdb
import pandas as pd

# Connect to DuckDB (creates an in-memory database)
con = duckdb.connect(database=':memory:')

# Create Employees table
employees_data = {'emp_id': [1, 2, 3, 4, 5],
                  'name': ['Alice', 'Bob', 'Charlie', 'David', 'Emma'],
                  'department': ['HR', 'IT', 'Sales', 'HR', 'Sales'],
                  'salary': [50000, 60000, 70000, 55000, 75000]}
employees_df = pd.DataFrame(employees_data)
con.register('employees', employees_df)

# Example Query with PARTITION BY
query = """
    SELECT
        name,
        department,
        salary,
        AVG(salary) OVER (PARTITION BY department) AS avg_salary_department
    FROM
        employees;
"""
result = con.execute(query).fetchdf()
print("Query with PARTITION BY:\n", result)

# Close the connection
con.close()
