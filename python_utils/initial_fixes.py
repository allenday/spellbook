import yaml
import os
import subprocess
import csv
from concurrent.futures import ThreadPoolExecutor
import re
import glob


def setup_csv(match0, match1, file_path, match_num):

    temp_table_fields = [field.strip() for field in match1.split(",") if field.strip()]
    
    # Remove comments at the beginning of the line
    no_comments = re.sub(r'^\s*--.*$', '', match0, flags=re.MULTILINE)

    # Remove comment lines from the data
    no_comments = re.sub(r'\)(\s*|\s*,\s*)--.*$', ')', no_comments, flags=re.MULTILINE)

    # change the array(values) to "[values]" with re
    array_fix = re.sub(r"array\((.*?)\)", '"[\1]"', no_comments, flags=re.MULTILINE)
    
    # This will check if a line ends with ')', if it does it corrects it to '),'
    clean_end_of_line = re.sub(r"\)\s*\n", "),\n", array_fix)

    # This will check if a line starts with ',(' and if it does it corrects it to '('
    clean_start_of_line = re.sub(r"^\s*,\s*\(", "(", clean_end_of_line, flags=re.MULTILINE) + ")"

    # Replace the matches of the first pattern
    no_commas = re.sub(r"(?<!['\"\)\d])\s*,", ' ', clean_start_of_line)

    # handle , in the middle of strings after "
    no_commas = re.sub(r'\"\s*,(?!\s*[\d\'\"\,])', '"', no_commas)

    # Replace the matches of the second pattern
    no_commas = re.sub(r"(?<!['\"\)])(\s*\d+\s*),(\s*)(?!\s*[,\"\'\d])", r'\1\2', no_commas)

    # Replace the matches of the third pattern
    no_commas = re.sub(r"(\d\s*),(\s*[\d\.]+\s*[\'\"a-zA-Z_%])", r'\1\2', no_commas)

    # take the quotes out of the strings so they aren't in the dbt strings
    # Removing starting single quotes
    no_quotes = re.sub(r"\(('|'')", "(", no_commas, flags=re.MULTILINE)

    # Removing single quotes after comma or newline
    no_quotes = re.sub(r",\s*('|\")", ", ", no_quotes)

    # Removing single quotes before comma or newline
    no_quotes = re.sub(r"('|\")\s*(,|\n)", " ,", no_quotes)

    # Removing single quotes before comma or newline or ) in case they were double ''
    no_quotes = re.sub(r"('|\")\s*(\))", r" )", no_quotes)

    final_string = no_quotes.replace(',', '').replace(',', '')

    # Split the rows correctly
    values = re.split(r'(?<=^)\s*(?=\()', final_string, flags=re.MULTILINE)[1:]
    
    data_pre = [[value.strip() for value in row.replace('(', '').replace(')', '').split(",")] for row in values] 
    
    data = []

    for row in data_pre[:-1]:
        data.append(row[:-1])
    
    data.append(data_pre[-1])

    filename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    directory = "seeds"
    os.makedirs(directory, exist_ok=True)
    model_name = filename_without_ext + "_" + str(match_num) + '_seed'
    csv_filename = os.path.join(directory, filename_without_ext + "_" + str(match_num) + '_seed.csv')

    with open(csv_filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(temp_table_fields)
                    writer.writerows(data)

    return model_name

# infers the type of the data being inputted
def infer_type(value):
    # remove comments
    value = re.sub(r"\)\s*--.*$", '', value)
    # erase the line if it starts with ( with any number of spaces before it
    value = re.sub(r"^\s*\(", '', value)
    lower_value = value.lower()
    if value.startswith("'") and value.endswith("'") or value.startswith('"') and value.endswith('"'):
        return "STRING"
    elif lower_value.startswith("timestamp") or lower_value.startswith("now()"):
        return "TIMESTAMP"
    elif lower_value.startswith("current_date") or lower_value.startswith("date"):
        return "DATE"
    elif "(" in value and ")" in value:
        data = re.findall(r'\((.*?)\)', value)
        if data[0].startswith("'") and data[0].endswith("'"):
            return "STRING"
        elif '.' in value:
            return "NUMERIC"  
        elif 'e' in value:
            return "BIGNUMERIC"        
        else:
            return "INT64"
    elif '.' in value:
        return "NUMERIC"  
    elif 'e' in value:
        return "BIGNUMERIC"
    elif value == "null":
        return "STRING"
    else:
        return "INT64"

def replace_materialized(file_path):
    # Open the SQL file
    with open(file_path, 'r+') as file:
        # Read the file content
        content = file.read()

        # Replace 'materialized = 'incremental'' with 'materialized = 'view''
        content = re.sub(
            r"materialized\s*=\s*'incremental'",
            "materialized = 'view'",
            content
        )
        
        # Replace 'materialized = 'table'' with 'materialized = 'view''
        content = re.sub(
            r"materialized\s*=\s*'table'",
            "materialized = 'view'",
            content
        )

        content = re.sub(
            r"incremental_stragegy\s*=\s*'merge',",
            "",
            content
        )

        # Write the updated content back to the file
        file.seek(0)
        file.write(content)
        file.truncate()

def fix_tests(file_path):
    # Start from the root of the 'models' directory
    root_dir = file_path

    # Walk through each file in the directory and its subdirectories
    for dir_name, subdirs, files in os.walk(root_dir):
        for filename in files:
            if filename.endswith('.yml'):
                file = os.path.join(dir_name, filename)

                # Read the YAML file
                with open(file, 'r') as read_file:
                    data = yaml.safe_load(read_file)

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
                with open(file, 'w') as write_file:
                    yaml.safe_dump(data, write_file)


def process_file(file_path):
    # Run the SQLFluff fix command on the file
    command = f'sqlfluff fix {file_path} --force'
    process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)

    # Replace 'materialized = 'incremental'' with 'materialized = 'view'' in the SQL file
    replace_materialized(file_path)

    # Return the output of the SQLFluff command
    return file_path, process.stdout


def fix_sqlfluff(file_path):
    
    # Prepare a list to store the file paths and outputs
    results = []
    # Define the reserved keywords.
    reserved_keywords = [
        "from",
        "to",
        "select",
        "where",
        "join",
        "inner",
        "outer",
        "left",
        "right",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "case",
        "when",
        "then",
        "else",
        "end",
        "and",
        "or",
        "not",
        "like",
        "as",
        "on",
        "in",
        "value",
        "day",
        "hour",
        "week",
        "HASH"
    ]
    # Start from the root of the 'models' directory
    root_dir = file_path
    print("root_dir: ", root_dir)
    # Walk through each file in the directory and its subdirectories
    sql_files = []
    left_bracket = '{'
    right_bracket = '}'
    for dir_name, subdirs, files in os.walk(root_dir):
        for filename in files:
            # print("filename: ", filename)
            
            # if filename != 'staking_ethereum_entities.sql':
            #     continue

            # Check if the file has a .sql extension
            if filename.endswith('.sql'):

                # Construct the full file path
                file_path = os.path.join(dir_name, filename)
                
                # Fix the from_values issues
                from_values_fix(file_path)

                # Add the file path to the list
                sql_files.append(file_path)
                
                # Read the SQL file
                with open(file_path, 'r+') as f:
                    content = f.read()
                    if filename == 'nft_ethereum_transfers.sql':
                        replacement = f"""
    SELECT 
        'ethereum' as blockchain,
        t.evt_block_time AS block_time,
        TIMESTAMP_TRUNC(DATE(t.evt_block_time), DAY) AS block_date,
        t.evt_block_number AS block_number,
        'erc1155' AS token_standard,
        'batch' AS transfer_type,
        t.evt_index,
        t.contract_address,
        t.id AS token_id,
        t.value AS amount,
        t.from,
        t.to,
        et.from AS executed_by,
        t.evt_tx_hash AS tx_hash,
        CONCAT('ethereum', t.evt_tx_hash, '-erc1155-', t.contract_address, '-', CAST(id AS STRING), '-', t.from, '-', t.to, '-', CAST(t.value AS STRING), '-', CAST(t.evt_index AS STRING)) AS unique_transfer_id
    FROM (
        SELECT 
            t.evt_block_time, 
            t.evt_block_number, 
            t.evt_tx_hash, 
            t.contract_address, 
            t.from, 
            t.to, 
            t.evt_index,
            id,
            value
        FROM 
            {left_bracket}{left_bracket} source('erc1155_ethereum', 'evt_transferbatch') {right_bracket}{right_bracket} t,
            UNNEST(t.ids) AS id WITH OFFSET id_offset,
            UNNEST(t.values) AS value WITH OFFSET value_offset
        WHERE 
            id_offset = value_offset
        {left_bracket}% if is_incremental() %{right_bracket}
            ANTI JOIN {left_bracket}{left_bracket}this{right_bracket}{right_bracket} anti_table
                ON t.evt_tx_hash = anti_table.tx_hash
        {left_bracket}% endif %{right_bracket}
        {left_bracket}% if is_incremental() %{right_bracket}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {left_bracket}% endif %{right_bracket}
        GROUP BY t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t.from, t.to, evt_index, id, value

    ) t
    INNER JOIN {left_bracket}{left_bracket} source('ethereum', 'transactions') {right_bracket}{right_bracket} et 
    ON et.block_number = t.evt_block_number
    WHERE t.value > 0
    AND et.hash = t.evt_tx_hash
    """                        
                        # Split content into lines, keep only the first 71 lines and join them back
                        content = content.split('\n')[:71]
                        content = '\n'.join(content) + replacement

                    elif filename == 'archipelago_ethereum_base_trades.sql':
                        original = """sum(amount) filter (
            where recipient not in ('0xa76456bb6abc50fb38e17c042026bc27a95c3314','0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9')
            ) as royalty_amount
        , sum(amount) filter (
            where recipient in ('0xa76456bb6abc50fb38e17c042026bc27a95c3314','0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9')
            ) as platform_amount"""
                        replacement = """SUM(CASE WHEN recipient NOT IN ('0xa76456bb6abc50fb38e17c042026bc27a95c3314', '0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9') THEN amount ELSE 0 END) AS royalty_amount
        , SUM(CASE WHEN recipient IN ('0xa76456bb6abc50fb38e17c042026bc27a95c3314', '0x1fc12c9f68a6b0633ba5897a40a8e61ed9274dc9') THEN amount ELSE 0 END) AS platform_amount"""
                        content = content.replace(original, replacement)

                    
                    elif filename == 'zeroex_ethereum_api_fills.sql':
                        # fix a double comparison issue
                        content = content.replace("INPUT","input")

                    elif filename == 'prices_usd_forward.sql':
                        # fix a double comparison issue
                        original = """, timeseries as (
    select explode(sequence(
        date_trunc('minute', now() - interval {{lookback_interval}})
        ,date_trunc('minute', now())
        ,interval 1 minute)) as minute
)"""
                        replacement = """, timeseries as (
    select timestamp_minute
    from UNNEST(GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{lookback_interval}} MINUTE), MINUTE), 
        TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), 
        INTERVAL 1 MINUTE)) as timestamp_minute
)"""
                        content = content.replace(original, replacement)

                    elif filename == 'prices_native_tokens.sql':
                        content = content.replace("CAST(decimals as int)", "CAST(decimals as int) as `decimals`")

                    elif filename == 'cow_protocol_ethereum_trades.sql':
                            original = """        distribute by
            evt_tx_hash, evt_block_number\n"""
                            replacement = ""
                            content = content.replace(original, replacement)

                    elif filename == 'balancer_ethereum_vebal_balances_day.sql':
                        original = """calendar AS (
        SELECT 
          explode(sequence(MIN(day), CURRENT_DATE, interval 1 day)) AS day
        FROM bpt_locked_balance
    ),"""                   
                        replacement = """calendar AS (
    SELECT TIMESTAMP(day) AS day
    FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(DATE(day)) FROM bpt_locked_balance), CURRENT_DATE(), interval 1 DAY)) AS `day`
),"""
                        content = content.replace(original, replacement)

                    elif filename == 'cryptopunks_ethereum_punk_transfers.sql':
                        content = content.replace("select  from", """select `from`""")

                    elif filename == 'zeroex_ethereum_api_fills_deduped.sql':
                        original = "CAST(ARRAY(-1) as array<bigint>)"
                        replacement = "ARRAY<INT64>[-1]"
                        content = content.replace(original, replacement)
                    
                    elif filename == 'label_eth_stakers.sql':
                        original = "LEFT ANTI JOIN identified_stakers is ON et.from = is.address"
                        replacement = "LEFT JOIN identified_stakers is ON et.from = is.address WHERE is.address IS NULL"
                        

                    original = """explode(ids) as explode_id,
      evt_tx_hash || '-' || cast(
        row_number() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) as string
      ) as unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}"""
                    replacement = """explode_id,
    evt_tx_hash || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY evt_tx_hash, ids ORDER BY ids) AS STRING) AS unique_transfer_id
  FROM
    `blocktrekker`.`erc1155_ethereum`.`evt_transferbatch`,
    UNNEST(ids) AS explode_id"""
                    content = content.replace(original, replacement)
                    original = """explode(
        values
      ) as explode_value,
      evt_tx_hash || '-' || cast(
        row_number() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) as string
      ) as unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}"""
                    replacement = """explode_value,
      evt_tx_hash || '-' || cast(
        row_number() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) as string
      ) as unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}},
    UNNEST(values) AS explode_value"""
                        
                    content = content.replace(original, replacement)
                

                    # fix a double comparison issue
                    content = content.replace("pcr.startBlock < pcr.evt_block_number < pcr.endBlock","pcr.startBlock < pcr.evt_block_number and pcr.evt_block_number < pcr.endBlock")
                    
                    content = re.sub(r"([<>]+)\s*'(\d+)'", r"\1\2", content)
                    content = re.sub(r"'(\d+)'\s*([<>]+)", r"\1\2", content)


                    # remove trailing whitespace and semicolon from last line
                    content = content.rstrip().rstrip(';').rstrip()

                    content = content.replace("CAST(ARRAY() as array<bigint>)", "ARRAY<BIGINT>[]")

                    # change the :: conversions to CAST
                    content = re.sub(
                        r'(\w+)::(\w+)',
                        r'CAST(\1 AS \2)',
                        content, 
                        flags=re.IGNORECASE
                    )
                    
                    # Replace the unhex() to TO_BASE64(FROM_HEX())
                    content = re.sub(
                        r'unhex\((.*?)\)',
                        r'TO_BASE64(FROM_HEX(\1))',
                        content,
                        flags=re.IGNORECASE
                    )                    

                    # Replace 'materialized = 'incremental'' with 'materialized = 'view''
                    content = re.sub(
                        r"materialized\s*=\s*'\w+'",
                        "materialized = 'view'",
                        content,
                        flags=re.IGNORECASE
                    )
                    
                    # Replace all the file_format statements with empty string
                    content = re.sub(
                        r"file_format\s*=\s*'delta',\n",
                        "",
                        content,
                        flags=re.IGNORECASE
                    )
                    
                    # Replace all the file_format statements with empty string
                    content = re.sub(
                        r"incremental_strategy = 'merge',\n",
                        "",
                        content,
                        flags=re.IGNORECASE
                    )

                    # Replace all the config statements with empty string
                    content = re.sub(
                        r",\s*post_hook\s*=.*?\)\s*\}\}\'",
                        '',
                        content,
                        flags=re.DOTALL
                    )
                    # a couple of special cases
                    # modular math special cases
                    content = content.replace("length(regexp_replace(data, '^.*00', ''))%2", "mod(length(regexp_replace(data, '^.*00', '')),2)")
                    content = re.sub(
                        r"([\w\.\_\d]+)\s*\%\s*([\w\.\_\d]+)",
                        r"MOD( \1, \2)",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = content.replace(
"""
    anti join reservoir r_b
        ON r_a.hash_marker != r_b.hash_marker
        and `right`(r_a.hash_marker, length(r_b.hash_marker)) = r_b.hash_marker
""",
"""
    LEFT JOIN reservoir r_b
        ON r_a.hash_marker = r_b.hash_marker
        AND ENDS_WITH(r_a.hash_marker, SAFE.SUBSTR(r_b.hash_marker, -LENGTH(r_b.hash_marker)))
    WHERE r_b.hash_marker IS NULL
"""                 )  
                    content = content.replace(
"""having count(distinct t.hash) filter(where t.`from` != '0x073ab1c0cad3677cde9bdb0cdeedc2085c029579') > 10
    and count(distinct nt.contract_address) > 2""",
"""HAVING 
    COUNT(DISTINCT IF(t.`from` != '0x073ab1c0cad3677cde9bdb0cdeedc2085c029579', t.hash, NULL)) > 10
    AND COUNT(DISTINCT nt.contract_address) > 2"""                 )
                    
                    # replace "UNION" with "UNION ALL", case-insensitively,
                    # but only if "UNION" is not already followed by " ALL"                    
                    content = re.sub(r'(?<=\s)UNION(?=\s)(?!\sALL\b)', 'UNION ALL', content, flags=re.IGNORECASE)
                    # find and replace date_trunc('interval', column) with TIMESTAMP_TRUNC(column, INTERVAL)
                    content = re.sub(
                        r"date_trunc\s*\(\s*\'(\w+)\'\s*,\s*([\w\.]+)\s*\)",  # pattern
                        r'TIMESTAMP_TRUNC(\2, \1)',  # replacement
                        content,
                        flags=re.IGNORECASE
                    )
                    # find and replace now() with CURRENT_TIMESTAMP()
                    content = re.sub(r'now\(\)', 'CURRENT_TIMESTAMP()', content, flags=re.IGNORECASE)



                    # find and replace explode(sequence(to_date(''), current_date, interval 1 day))
                    content = re.sub(
                        r"(?i)(\w+)\s*AS\s*\(\s*SELECT\s*explode\(sequence\(MIN\((\w+)\),\s*CURRENT_DATE,\s*interval\s*1\s*day\)\)\s*AS\s*(\w+)\s*FROM\s*(\w+)\s*\)",
                        r"\1_pre AS (\n    SELECT MIN(DATE(\2)) as \2\n    FROM \4\n),\n\1 AS (\n    SELECT \2\n    FROM UNNEST(GENERATE_DATE_ARRAY((SELECT \2 FROM \1_pre), CURRENT_DATE(), INTERVAL 1 DAY)) AS \2\n)",
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace explode(sequence(to_date(''), current_date, interval 1 day))
                    content = re.sub(
                        r"(?i)(\w+)\s*AS\s*\(\s*SELECT\s*explode\(sequence\(MIN\((\w+)\),\s*CURRENT_DATE,\s*interval\s*1\s*weed\)\)\s*AS\s*(\w+)\s*FROM\s*(\w+)\s*\)",
                        r"\1_pre AS (\n    SELECT MIN(DATE(\2)) as \2\n    FROM \4\n),\n\1 AS (\n    SELECT \2\n    FROM UNNEST(GENERATE_DATE_ARRAY((SELECT \2 FROM \1_pre), CURRENT_DATE(), INTERVAL 7 DAY)) AS \2\n)",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = re.sub(
                        r"(?i)(explode\(sequence\(\s*to_date\('([^']+)'\),\s*CURRENT_DATE,\s*INTERVAL\s+1\s+WEEK\)\))",
                        r"* FROM UNNEST(GENERATE_DATE_ARRAY(DATE('\2'), CURRENT_DATE(), INTERVAL 1 WEEK))",                        
                        content,
                        flags=re.IGNORECASE
                    )
                    
                    content = re.sub(
                        r"(?i)(explode\(sequence\(\s*to_date\('([^']+)'\),\s*current_date,\s*interval\s+1\s+day\)\))",
                        r"TIMESTAMP(day) as day FROM UNNEST(GENERATE_DATE_ARRAY(DATE('\2'), CURRENT_DATE(), INTERVAL 1 DAY))",
                        content,
                        flags=re.IGNORECASE
                    )           

                    
                    # find and replace partition_by in list form with a dict
                    content = re.sub(
                        r'partition_by\s*=\s*\[\s*\'\s*([^\']*)\s*\'\s*\]',
                        r'partition_by = {"field": "\1"}',
                        content,
                        flags=re.IGNORECASE
                        )
                    
                    # find and replace posexplode with unnest functions
                    content = re.sub(
                        r"LATERAL\s+VIEW\s+posexplode\(([\w\.\_]+)\)\s+([\w\.\_]+)\s+AS\s+pos,\s+([\w\.\_]+)\s+LATERAL\s+VIEW\s+posexplode\(([\w\.\_]+)\)\s+([\w\.\_]+)\s+AS\s+pos,\s+([\w\.\_]+)\s+WHERE\s+\2\.pos\s+=\s+\5\.pos",
                        r", UNNEST(\1) AS \3 WITH OFFSET AS \2\n, UNNEST(\4) AS \6 WITH OFFSET AS \5\nWHERE \3.\2 = \6.\5",
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace posexplode with unnest functions
                    content = re.sub(
                        r"LATERAL\s+VIEW\s+posexplode\(([\w\.\_]+)\)\s*([\w\.\_]+)\s+AS\s+([\w\.\_]+),\s+([\w\.\_]+)\s+LATERAL\s+VIEW\s+posexplode\(([\w\.\_]+)\)\s+([\w\.\_]+)\s+AS\s+([\w\.\_]+),\s+([\w\.\_]+)\s+WHERE\s+\3\s+=\s+\7",
                        r", UNNEST(\1) AS \4 WITH OFFSET AS \3\n, UNNEST(\5) AS \8 WITH OFFSET AS \7\nWHERE \3 = \7",
                        content,
                        flags=re.IGNORECASE
                    )


                    # find and replace DOUBLE with Float64
                    content = re.sub(
                        r"(\bDOUBLE\b)",
                        r"FLOAT64",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = content.replace('sort by', 'order by')

                    # find and replace any misused reserved keywords
                    content = re.sub(
                        r"(?i)\bAS\b\s+\b(" + "|".join(reserved_keywords) + r")\b",
                        r"AS `\1`",
                        content,
                        flags=re.IGNORECASE
                    )

                    # apply for each reserved keyword separately
                    for keyword in reserved_keywords:
                        # find and replace any misused reserved keywords
                        content = re.sub(
                            r"(?i)(\|\|\s+|\,\s+|AS\s+|distinct\s+)\b" + keyword + r"\b(\s+\|\||\s*,|\sAS\s)",
                            r"\1`" + keyword + r"`\2",
                            content,
                            flags=re.IGNORECASE | re.MULTILINE
                        )
                        content = re.sub(
                            r"(?i)(WHERE\s+|OR\s+|AND\s+)\b" + keyword + r"(\bIN\b|\s*=\s*|\bIS\s*|\s*\!=\s*)",
                            r"\1`" + keyword + r"`\2",
                            content,
                            flags=re.IGNORECASE | re.MULTILINE
                        )
                        content = re.sub(
                            r"(?i)(GROUP BY\s+|ORDER BY\s+)\b" + keyword + r"\b",
                            r"\1`" + keyword + "`",
                            content,
                            flags=re.IGNORECASE | re.MULTILINE
                        )

                        
                    # find and replace any SUBSTRING functions with SUBSTR    
                    content = re.sub(
                        r'SUBSTRING\s*\(\s*(.*?)\s+FROM\s+(\d+)\s+FOR\s+(\d+)\s*\)',
                        r'SUBSTR(\1, \2, \3)',
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace any SUBSTRING functions with SUBSTR and no FOR  
                    content = re.sub(
                        r'SUBSTRING\s*\(\s*(.*?)\s+FROM\s+(\d+)\s*\)',
                        r'SUBSTR(\1, \2)',
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace any CASTs to Numeric(num)
                    content = re.sub(
                        r"(?i)CAST\(([\w\d]+?) AS DECIMAL\(\s*[\d,\s*]+\)\)",
                        r"CAST(\1 AS BIGNUMERIC)",
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace any CASTs to Numeric(num)
                    content = re.sub(
                        r"(?i)CAST\(([\w\d]+?) AS NUMERIC\(\s*[\d,\s*]+\)\)",
                        r"CAST(\1 AS BIGNUMERIC)",
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace any TRY_CASTs to SAFE_CAST
                    content = re.sub(
                        r'\bTRY_CAST\b',
                        r'SAFE_CAST',
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace UNIX_TIMESTAMP functions
                    content = re.sub(
                        r"(?i)unix_timestamp\((.*?)\)",
                        r"UNIX_SECONDS(TIMESTAMP(\1))",
                        content,
                        flags=re.IGNORECASE
                    )

                    # Add the udfs prefix to any bytea2numeric functions
                    content = re.sub(
                        r'\b(?!udfs\.)(bytea2numeric)\b',
                        r'udfs.\1',
                        content,
                        flags=re.IGNORECASE
                    )

                    # Add the udfs prefix to any bytea2numeric functions
                    content = re.sub(
                        r'\b(?!udfs\.)(bytea2numeric_v3)\b',
                        r'udfs.\1',
                        content,
                        flags=re.IGNORECASE
                    )


                    content = re.sub(
                        r'VARCHAR\([^\)]*\)',
                        'STRING',
                        content,
                        flags=re.IGNORECASE
                    )

                    content = re.sub(
                        r"\binterval\s+(\d+\s+\w+)",
                        r"interval \1", 
                        content, 
                        flags=re.IGNORECASE
                    )
                    # replace max_by with array_agg
                    content = re.sub(
                        r"max_by\(\s*([\w\.]+)\s*,\s*([\w\._]+)\s*\)",
                        r"ARRAY_AGG(\1 ORDER BY \2 DESC LIMIT 1)[OFFSET(0)]", 
                        content, 
                        flags=re.IGNORECASE
                    )
                    # replace min_by with array_agg
                    content = re.sub(
                        r"min_by\(\s*([\w\.]+)\s*,\s*([\w\._]+)\s*\)",
                        r"ARRAY_AGG(\1 ORDER BY \2 ASC LIMIT 1)[OFFSET(0)]", 
                        content, 
                        flags=re.IGNORECASE
                    )
                    # replace FIRST with array_agg
                    content = re.sub(
                        r"\bFIRST\(\s*([\w\.]+)\s*\)",
                        r"ARRAY_AGG(\1 ORDER BY \1 LIMIT 1)[OFFSET(0)]",
                        content,
                        flags=re.IGNORECASE
                    )
                    # Replace the minute section of the partition by with the new function
                    content = re.sub(
                        r"(ON|AND)\s+(\w+\.\w+)\s*=\s*TIMESTAMP_TRUNC\(DATE\((\w+\.\w+)\), minute\)",
                        r"\1 \2 = TIMESTAMP_SECONDS(CAST((UNIX_SECONDS(\3) / 60) * 60 AS INT64))",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = re.sub(
                        r"POSITION\('([\w\d]+)' IN ([\w\d]+)\)",
                        r"STRPOS(\2, '\1')",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = re.sub(
                        r"SUBSTRING\(([\w\.\_]+)\s*FROM\s*\((.+?)\)\s*FOR\s*(\d+)\)",
                        r"SUBSTR(\1, \2, \3)",
                        content,
                        flags=re.IGNORECASE
                    )
                     
                    # fix coalesce function to replace the -1
                    content = content.replace(
                    "COALESCE(nft_mints.token_id, '-1') || '-' || COALESCE(nft_mints.amount, '-1') || '-'|| COALESCE(erc20s.contract_address, '0x0000000000000000000000000000000000000000') || '-' || COALESCE(nft_mints.evt_index, '-1')",
                    "COALESCE(nft_mints.token_id, -1) || '-' || COALESCE(nft_mints.amount, -1) || '-'|| COALESCE(erc20s.contract_address, '0x0000000000000000000000000000000000000000') || '-' || COALESCE(nft_mints.evt_index, -1)"
                    )

                    content = content.replace(
                    "lower(concat(array_join(collect_list(symbol), '/'), ' ', array_join(collect_list(cast(norm_weight AS string)), '/')))",
                    "\nCONCAT(STRING_AGG(symbol, '/') , ' ' , STRING_AGG(CAST(norm_weight AS STRING), '/'))"
                    )
                    
                    content = content.replace(
                        "WHERE token_address.tokens = normalized_weight.weights",
                        "WHERE tokens = weights"
                    )

                    content = content.replace(
                        "tokens.token_address",
                        "token_address"
                    )

                    content = content.replace(
                        "weights.normalized_weight",
                        "normalized_weight"
                    )

                    content = content.replace(
                        "SUBSTRING(registered.poolId, 0, 42)",
                        "CAST(SUBSTRING(registered.poolId, 0, 42) as STRING)"
                    )

                    content = content.replace(
                        "AND denorm <> '0'",
                        "AND denorm <> 0"
                    )

                    content = content.replace(
                        "date_add(start_date, 7)",
                        "date_add(start_date, interval 7 day)"
                    )

                    content = content.replace(
                        "'0' AS denorm",
                        "0 AS denorm"
                    )

                    content = content.replace(
                        "CAST(events.denorm AS FLOAT64)",
                        "CAST(events.denorm AS FLOAT64) as denorm"
                    )             
                    
                    content = content.replace(
                        "SUM(denorm) AS sum_denorm",
                        "SUM(denorm) AS sum_denorm_"
                    )

                    content = content.replace(
                        "denorm / sum_denorm AS normalized_weight",
                        "denorm / sum_denorm_ AS normalized_weight"
                    )
                    content = content.replace(
                        "ON v.evt_block_time >= r.start_date",
                        "ON v.evt_block_time >= timestamp(r.start_date)",
                    )
                    content = content.replace(
                        "v.evt_block_time < r.end_date",
                        "v.evt_block_time < timestamp(r.end_date)"
                    )
                    content = content.replace(
                        "call_create.output_0 = CAST(SUBSTRING(registered.poolId, 0, 42) as STRING)",
                        "lower(call_create.output_0) = lower(CAST(SUBSTRING(registered.poolId, 0, 42) as STRING))"
                    )

                    content = content.replace(
                        "get_json_object",
                        "JSON_EXTRACT_SCALAR"
                    )

                    content = content.replace(
                        "collect_list",
                        "ARRAY_AGG" 
                    )

                    content = content.replace(
                        "array_join",
                        "STRING_AGG" 
                    )
                    # make sure float is FLOAT64
                    content = content.replace(
                        " float",
                        " FLOAT64"
                    )
                    # replace from_unixtime with timestamp_seconds
                    content = content.replace(
                        "from_unixtime",
                        "timestamp_seconds"
                    )
                    # fix numeric() and decimal() to be bignumeric
                    content = re.sub(
                        r"\b(?:NUMERIC|DECIMAL)\s*\([\w\d,]+\)",
                        "BIGNUMERIC",
                        content,
                        flags=re.IGNORECASE
                    )

                    content = re.sub(
                        r"split_part\(([\w\_\.]+)\s*,\s*([\W_]+)\s*,\s*([\d]+)\)",
                        r"SPLIT\(\1, \2\)\[SAFE_OFFSET(\3)\]",
                        content,
                        flags=re.IGNORECASE
                    )

                    # find and replace any missused reserved keywords in the middle of a cast
                    content = re.sub(
                        r"(?i)CAST\s*\(\s*(" + "|".join(reserved_keywords) + r")\s*AS\s*([\w\d]+)\s*\)",
                        lambda match: f"CAST(`{match.group(1)}` AS {match.group(2)})",
                        content,
                        flags=re.IGNORECASE
                    )

                    # fix anti joins
                    content = re.sub(r"LEFT ANTI JOIN (\w+) (\w+)\s+ON", r"LEFT JOIN \1 \2 ON", content)
                    content = re.sub(r"WHERE (\w+)\.(\w+) =", r"WHERE \1.\2 IS NULL AND \1.\2 =", content)

                    # fix the label_eth_stakers.sql file
                    content = content.replace(
                        "LEFT ANTI JOIN identified_stakers is ON et.from = is.address", 
                        "LEFT JOIN identified_stakers is ON et.from = is.address WHERE is.address IS NULL")

 
                    # wrong use of topic
                    content = content.replace("topic2","topic1")
                    content = content.replace("topic3","topic2")
                    content = content.replace("topic4","topic3")

 
                # write the new content back to the file
                    f.seek(0)
                    f.write(content)
                    f.truncate()

    # # Process the files using multiple threads
    # with ThreadPoolExecutor() as executor:
    #     futures = [executor.submit(process_file, file_path) for file_path in sql_files]

    #     for future in futures:
    #         try:
    #             result = future.result()
    #             if result is not None:  # Only append result if it's not None
    #                 results.append(result)
    #         except Exception as e:
    #             print(f"Error processing file: {e}")

    # # Save the results to a CSV file
    # with open('sqlfluff_output.csv', 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow(['File', 'Output'])
    #     for result in results:
    #         writer.writerow(result)

def from_values_fix(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        if len(content) > 250000:
            long_sql_fixes(file_path)
            # print("Long SQL file: " + file_path)
            return
        # Remove comments at the beginning of the line
        content = re.sub(r'^\s*--.*$', '', content, flags=re.MULTILINE)

        # Remove comment lines from the data
        content = re.sub(r'--.*$',"", content, flags=re.MULTILINE)
        
        # fix one off formatting errors to proper formatting
        if  file_path == "models/cow_protocol/ethereum/cow_protocol_ethereum_solvers.sql" or file_path == "models/cow_protocol/gnosis/cow_protocol_gnosis_solvers.sql":
            content = content.replace(") as _", "").replace("SELECT *","").replace("FROM (","")
        # fix a one off issues with )) in a name
        if file_path == "models/tokens/avalanche_c/tokens_avalanche_c_nft_curated.sql":
            content = content.replace("))", ")")

        # fix a one off issues with ) in a name
        if file_path == "models/addresses/optimism/addresses_optimism_grants_funding.sql":
            content = content.replace("-- Distributions to Rewarder contracts are not listed", "")

        pattern_as = r"(?s)(\(([\w\s,]+)\)\s*as\s*\(\s*VALUES\s*(.*?\))\s+\))"
    
        matches_as = re.findall(pattern_as, content, re.DOTALL | re.IGNORECASE | re.MULTILINE)

    if matches_as:
        # print("Match found in file: " + file_path)
        for match in matches_as:
            # # print("match 1:   " + match[1])
            try:
                temp_table_fields = [field.strip() for field in match[1].split(",") if field.strip()]
            except:
                print("temp-error")
                print("match:  " + str(match))
            
            # Remove comments at the beginning of the line
            no_comments = re.sub(r'^\s*--.*$', '', match[2], flags=re.MULTILINE)

            # Remove comment lines from the data
            no_comments = re.sub(r'\)(\s*|\s*,\s*)--.*$', ')', no_comments, flags=re.MULTILINE)

            # Remove double '' in sql statments
            no_quotes = re.sub(r'(?<![,)])\s*\'\'\s*(?![,)])', '', no_comments)

            # eliminates line breaks between values
            no_quotes = re.sub(r'(?<!\)\,)(?<!\))\s*\n\s*', '', no_quotes, flags=re.MULTILINE)

            # eliminates the first comma and parenthesis
            no_quotes = re.sub(r'^\s*,\s*', '', no_quotes, flags=re.MULTILINE)

            # makes sure the line ends in a comma
            no_quotes = re.sub(r'\)\s*$', '),', no_quotes, flags=re.MULTILINE)

            # eliminates a trailing ), on the last line if its alone and always deletes the last character which is a comma
            trailing_para = re.sub(r'\n\)\,$', '', no_quotes)[:-1]
            
            # add structs to the beginning of the lines of values from pattern
            struct = (re.sub(r'\),\s*\(', '),\nSTRUCT(', trailing_para))
            add_struct = ("STRUCT" + struct)
            
            # Split the rows correctly
            values = re.split(r',\s*\(', no_quotes)
            values = ['(' + v for v in values if v]
            data = [[value.strip() for value in row.replace('((','(').strip()[1:-1].split(",")] for row in values]            
            struct_list = []

                # STRUCT<pool STRING, token_id STRING, token_address STRING>

            index = 0
            for value in data[0]:
                try:
                    struct_list.append(f"{temp_table_fields[index]} {infer_type(value)}")
                except IndexError:
                    print("IndexError: " + file_path)
                index += 1
            struct_string = f"ARRAY<STRUCT<{','.join(struct_list)}>>"

            input_string = f"AS (SELECT * FROM UNNEST({struct_string} [{add_struct}]))"


            sanitized_match = re.sub("\\\\L", "", match[0])
            sanitized_content = re.sub("\\\\L", "", content)
            sanitized_input_string = re.sub("\\\\L", "", input_string)

            try:
                content = re.sub(re.escape(sanitized_match), sanitized_input_string, sanitized_content)
            except re.error as e:
                print("An error occurred during regex substitution.")
                print(f"Error message: {e}")
                raise

        with open(file_path, 'w') as sql_file:
            sql_file.write(content)                 
    
    # Pattern to match SQL table creation
    pattern_from = r"(?s)((?:FROM|from)\s*\(\s*VALUES\s*(.*?\))\s*\)\s*(?:AS\s*|[\w*])\s*.*?\(([\w\s,`]+)\))"
    # Extract the matches
    matches_from = re.findall(pattern_from, content, re.DOTALL | re.IGNORECASE | re.MULTILINE)
    if matches_from:
        # print("Match found in file: " + file_path)
        for match in matches_from:
            # print("match 1:   " + match[1])
            try:
                temp_table_fields = [field.strip() for field in match[2].split(",") if field.strip()]
            except:
                print("error")
                # print("match:  " + str(match))
            
            # Remove comments at the beginning of the line
            no_comments = re.sub(r'^\s*--.*$', '', match[1], flags=re.MULTILINE)

            # Remove comment lines from the data
            no_comments = re.sub(r'\)(\s*|\s*,\s*)--.*$', ')', no_comments, flags=re.MULTILINE)

            # Remove double '' in sql statments
            no_quotes = re.sub(r'(?<![,)])\s*\'\'\s*(?![,)])', '', no_comments)

            # eliminates line breaks between values
            no_quotes = re.sub(r'(?<!\)\,)(?<!\))\s*\n\s*', '', no_quotes, flags=re.MULTILINE)

            # eliminates the first comma and parenthesis
            no_quotes = re.sub(r'^\s*,\s*', '', no_quotes, flags=re.MULTILINE)

            # makes sure the line ends in a comma
            no_quotes = re.sub(r'\)\s*$', '),', no_quotes, flags=re.MULTILINE)

            # eliminates a trailing ), on the last line if its alone and always deletes the last character which is a comma
            trailing_para = re.sub(r'\n\)\,$', '', no_quotes)[:-1]
            
            # add structs to the beginning of the lines of values from pattern
            struct = (re.sub(r'\),\s*\(', '),\nSTRUCT(', trailing_para))
            add_struct = ("STRUCT" + struct)
            
            # Split the rows correctly
            values = re.split(r',\s*\(', no_quotes)
            values = ['(' + v for v in values if v]
            data = [[value.strip() for value in row.replace('((','(').strip()[1:-1].split(",")] for row in values]            
            struct_list = []

                # STRUCT<pool STRING, token_id STRING, token_address STRING>

            index = 0
            for value in data[0]:
                try:
                    struct_list.append(f"{temp_table_fields[index]} {infer_type(value)}")
                except IndexError:
                    print("IndexError: " + file_path)
                index += 1
            struct_string = f"ARRAY<STRUCT<{','.join(struct_list)}>>"

            input_string = f"FROM UNNEST({struct_string} [{add_struct}])"

            sanitized_match = re.sub("\\\\L", "", match[0])
            sanitized_content = re.sub("\\\\L", "", content)
            sanitized_input_string = re.sub("\\\\L", "", input_string)

            try:
                content = re.sub(re.escape(sanitized_match), sanitized_input_string, sanitized_content)
            except re.error as e:
                print("An error occurred during regex substitution.")
                print(f"Error message: {e}")
                raise
                     


        with open(file_path, 'w') as sql_file:
            sql_file.write(content)
    

def superrare_ethereum_usernames_seed():
    with open('models/superrare/superrare_ethereum_usernames.sql', 'r') as file:
        content = file.read()

    pattern = r"(?s)(?:FROM\s*\(\s*VALUES\s*|SELECT\s*[\w\s,]+\s+FROM\s*\(\s*VALUES\s*)(.*?)(?:\)\s*AS\s*.*?\(([\w\s,]+)\)|$)"
    # Extract the matches
    matches = re.findall(pattern, content, re.IGNORECASE)

    # If matches are found, prepare and write data to CSV
    if matches:
        for match in matches:
            temp_table_fields = [field.strip() for field in match[1].split(",") if field.strip()]
            # Remove "updated_at" from the fields
            temp_table_fields.remove("updated_at")

            # Remove comment lines from the data
            no_comments = re.sub(r'--.*$', '', match[0], flags=re.MULTILINE)

            # Replace 'date ('2022-10-01')' with '2022-10-01'
            no_date_function = re.sub(r"date\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s*\)", r'\1', no_comments)

            # Remove ', now()'
            no_timestamp = re.sub(r',\s*now\(\)', '', no_date_function)

            # Split the rows correctly
            values = re.split(r',\s*\(', no_timestamp)
            values = ['(' + v for v in values if v]

            data = [[value.strip() for value in row.replace('(', '').replace(')', '').split(",")] for row in values] # Remove the last column from each row
            # Prepare filename and create "tables" directory if it doesn't exist
            filename_without_ext = os.path.splitext(os.path.basename('models/superrare/superrare_ethereum_usernames.sql'))[0]
            directory = "seeds"
            os.makedirs(directory, exist_ok=True)
            csv_filename = os.path.join(directory, filename_without_ext + '_seed.csv')

            with open(csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(temp_table_fields)
                writer.writerows(data)
        
        new_sql_content = re.sub(pattern,"SELECT blockchain, address, name, category, contributor, source, created_at, CURRENT_TIMESTAMP() AS updated_at\nFROM {{ref('superrare_ethereum_usernames_seed')}}", content, re.IGNORECASE)
        
        with open('models/superrare/superrare_ethereum_usernames.sql', 'w') as sql_file:
            sql_file.write(new_sql_content)

def genesis_balances_seed():
    with open('models/balances/ethereum/genesis/genesis_balances.sql', 'r') as file:
        content = file.read()

    pattern = r"(?s)(?:FROM\s*\(\s*VALUES\s*|SELECT\s*[\w\s,]+\s+FROM\s*\(\s*VALUES\s*)(.*?)(?:\)\s*AS\s*.*?\(([\w\s,]+)\)|$)"
    # Extract the matches
    matches = re.findall(pattern, content, re.IGNORECASE)

    # If matches are found, prepare and write data to CSV
    if matches:
        for match in matches:
            temp_table_fields = [field.strip() for field in match[1].split(",") if field.strip()]
 
            # Remove comment lines from the data
            no_comments = re.sub(r'\)\s*,\s*--.*$', '', match[0], flags=re.MULTILINE)
            # Replace 'CAST('1337000000000000000000' AS DECIMAL(38,0))' with 1337000000000000000000
            no_cast = re.sub(r"cast\s*\(\s*'(\d+)'\s*AS\s*DECIMAL\(38,0\)\s*\)", r'\1', no_comments, flags=re.IGNORECASE)
            # Split the rows correctly
            values = re.split(r',\s*\(', no_cast)

            values = ['(' + v for v in values if v]


            data = [[value.strip() for value in row.replace('(', '').replace(')', '').split(",")] for row in values]

            # Prepare filename and create "tables" directory if it doesn't exist
            filename_without_ext = os.path.splitext(os.path.basename('models/balances/ethereum/genesis/genesis_balances.sql'))[0]
            directory = "seeds"
            os.makedirs(directory, exist_ok=True)
            csv_filename = os.path.join(directory, filename_without_ext + '_seed.csv')

            with open(csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(temp_table_fields)
                writer.writerows(data)
            
        new_sql_content = re.sub(
            pattern,"""SELECT 
	address,
	CAST(balance_raw AS NUMERIC) AS balance_raw
	CAST(balance_raw AS NUMERIC) / power(10,18) as balance 
FROM 
	{{ ref ( 'genesis_balances_seed' )}}""", 
    content, 
    re.IGNORECASE)
        with open('models/balances/ethereum/genesis/genesis_balances.sql', 'w') as sql_file:
            sql_file.write(new_sql_content)

def long_sql_fixes(file_path):

    with open(file_path, 'r') as file:
        content = file.read()

    pattern = r"(?s)(?:FROM\s*\(\s*VALUES\s*|SELECT\s*[\w\s,]+\s+FROM\s*\(\s*\(\s*VALUES\s*)(.*?)(?:\)\s*\)\s*AS\s*.*?\(([\w\s,]+)\)|$)"
    # Extract the matches
    matches = re.findall(pattern, content, re.IGNORECASE)
    # If matches are found, prepare and write data to CSV
    if matches:
            match_num = 0
            for match in matches:

                model_name = setup_csv(match[0], match[1], file_path, match_num)
                match_num += 1               

            pattern_2 = r"(?s)(?:FROM\s*\(\s*VALUES\s*|FROM\s*\(\s*VALUES\s*)(.*?)(?:\(\s*\(\s*AS\s*.*?\(([\w\s,]+)\)|$)"
            
            left_bracket = '{'
            right_bracket = '}'
            new_sql_content = re.sub(pattern_2, f"FROM\n  {left_bracket}{left_bracket} ref( '{model_name}' ) {right_bracket}{right_bracket}", content, flags=re.IGNORECASE)
                
            with open(file_path, 'w') as sql_file:
                sql_file.write(new_sql_content)
            


file_path = 'models'

# Fix the tests in the YAML file
fix_tests("models/")

# Fix the abnormal SQL files
superrare_ethereum_usernames_seed()
genesis_balances_seed()


# Fix the SQL files using SQLFluff
fix_sqlfluff(file_path)

