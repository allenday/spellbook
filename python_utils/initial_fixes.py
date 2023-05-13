import yaml
import os
import subprocess
import csv
from concurrent.futures import ThreadPoolExecutor


def fix_tests():
    # Path to your YAML file
    file_path = 'models/tokens/optimism/tokens_optimism_schema.yml'

    # Read the YAML file
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)

    # Traverse each model under 'models' in the YAML file
    for model in data.get('models', []):

        # Check and fix 'tests' where it's None in columns
        for column in model.get('columns', []):
            if column.get('tests') is None:
                column['tests'] = []

        # Check and fix 'tests' where it's None at model level
        if model.get('tests') is None:
            model['tests'] = []

    # Write the updated data back to the YAML file
    with open(file_path, 'w') as file:
        yaml.safe_dump(data, file)


def process_file(file_path):
    # Run the SQLFluff fix command on the file
    command = f'sqlfluff fix {file_path} --force'
    process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)

    # Return the output of the SQLFluff command
    return file_path, process.stdout


def fix_sqlfluff():
    # Prepare a list to store the file paths and outputs
    results = []

    # Start from the root of the 'models' directory
    root_dir = 'models'

    # Walk through each file in the directory and its subdirectories
    sql_files = []
    for dir_name, subdirs, files in os.walk(root_dir):
        for filename in files:

            # Check if the file has a .sql extension
            if filename.endswith('.sql'):

                # Construct the full file path
                file_path = os.path.join(dir_name, filename)

                # Add the file path to the list
                sql_files.append(file_path)

    # Process the files using multiple threads
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, file_path) for file_path in sql_files]

        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error processing file: {e}")

    # Save the results to a CSV file
    with open('sqlfluff_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Output'])
        for result in results:
            writer.writerow(result)


# Fix the tests in the YAML file
fix_tests()

# Fix the SQL files using SQLFluff
fix_sqlfluff()

