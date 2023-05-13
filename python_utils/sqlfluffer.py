import os
import json
import subprocess




def lint_sql_code(sql_code: str):
    with open('temp.sql', 'w') as f:
        f.write(sql_code)

    # run sqlfluff linting on the SQL file
    result = subprocess.run(['sqlfluff', 'lint', 'temp.sql'], capture_output=True, text=True)

    return result.stdout


# Traverse the directory and subdirectories to find all files ending in .sql
for subdir, dirs, files in os.walk('/home/outsider_analytics/Code/spellbook-may/models/balancer/arbitrum'):
    count = 0
    for file in files:
        count = count + 1
        if file.endswith('.sql'):
            # run sqlfluffer on the file
            
            
            # Convert the SQL query to a Big Query compatible version
            
            # Write the Big Query compatible version to the file
            
            if count == 2:
                break
