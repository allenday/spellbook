# Import the required modules
import os
import yaml
import csv
import re

# Define a function to extract tables from a YAML file
def extract_tables_from_yml(yml_file_path):
    # Open the YAML file in read mode
    with open(yml_file_path, "r") as f:
        # Load the YAML content as a dictionary
        yml_content = yaml.safe_load(f)
        # Loop through the "sources" list in the YAML content
        for source in yml_content.get("sources", []):
            # Get the name of the source
            source_name = source.get("name")
            # Find the last underscore in the source name
            last_underscore_index = source_name.rfind("_")
            # If the source name ends with "_ethereum", modify it
            if source_name[last_underscore_index:] == "_ethereum":
                source_name = source_name[:last_underscore_index]
                print(f"Found ethereum source: {source_name}")
            # If the source name is "ethereum", skip it
            elif source_name == "ethereum":
                pass
            # If the source name doesn't end with "_ethereum", skip it
            else:
                continue
            # Loop through the "tables" list in the source
            for table in source.get("tables", []):
                # Get the name of the table
                table_name = table.get("name")
                # If the table name is not empty, add it to the list of all tables
                if table_name:
                    all_tables.append(table_name)
                    print(table_name)


def extract_tables_from_sql(sql_file_path):
    with open(sql_file_path, "r") as f:
        sql_content = f.read()
        pattern = "{{ source('(.+?)','(.+?)') }}"
        regex = re.compile(pattern)
        for match in regex.findall(sql_content):
            schema_name = match[0]
            table_name = match[1]
            if schema_name.endswith("_ethereum"):
                    all_tables.append(table_name)

# Define a function to find YAML files in a directory
def find_yml_files(directory):
    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        # Loop through the files in the current directory
        for file in files:
            # If the file ends with "sources.yml", process it
            if file.endswith("sources.yml"):
                # Get the full path of the YAML file
                yml_file_path = os.path.join(root, file)
                # Extract the tables from the YAML file
                tables = extract_tables_from_yml(yml_file_path)
                # If tables were found, yield them
                if tables:
                    yield tables

# Define a function to find sql files in a directory
def find_sql_files(directory):
    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        # Loop through the files in the current directory
        for file in files:
            # If the file ends with ".sql", process it
            if file.endswith(".sql"):
                # Get the full path of the sql file
                sql_file_path = os.path.join(root, file)
                # Extract the tables from the sql file
                tables = extract_tables_from_sql(sql_file_path)
                # If tables were found, yield them
                if tables:
                    yield tables


# Define a function to write tables to a CSV file
def write_tables_to_csv(output_path, tables):
    # Open the CSV file in write mode
    with open(output_path, "w") as f:
        # Create a CSV writer object
        writer = csv.writer(f)
        # Write the header row
        writer.writerow(["Table Name"])
        # Loop through the tables and write them to the CSV file
        for table in tables:
            writer.writerow([table])

# Set the directory path and initialize the list of all tables
directory_path = "models"
all_tables = []

# Loop through the tables found in the YAML files and add them to the list of all tables
for tables in find_yml_files(directory_path):
    all_tables.extend(tables)

# If any tables were found, write them to a CSV file
if all_tables:
    write_tables_to_csv("tables/spellbook_dependent_tables.csv", all_tables)
# If no tables were found, print a message
else:
    print(f"No tables found in {directory_path}")