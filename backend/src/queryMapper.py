import re
import string
import nltk

# Schema for the dataset remains unchanged
schema_structure = {
    "year": ["year", "model year", "manufacturing year", "production year", "release year", "launch year", "publication year", "production year"],
    "manufacturer": ["company", "brand", "manufacturer", "automaker"],
    "model_name": ["model", "car model", "vehicle model", "make", "cars", "model of car", "car sold", "car"],
    "body_type": ["body", "car body", "vehicle type", "chassis"],
    "transmission_type": ["transmission", "gearbox", "drive system", "transmission type"],
    "mileage": ["odometer", "mileage", "distance traveled", "miles"],
    "paint_color": ["color", "paint", "shade", "hue"],
    "interior_design": ["interior", "inside", "cabin", "seating"],
    "sale_price": ["selling price of car", "price", "cost", "value", "price sold"],
    "game_title": ["title", "game name", "video game title", "game", "gamename", "games"],
    "gaming_platform": ["console", "gaming platform", "system", "device"],
    "game_genre": ["category", "game type", "game category", "style", "genre of games"],
    "game_publisher": ["developer", "game publisher", "studio", "producer", "publisher"],
    "na_sales": ["North America sales", "NA revenue", "USA sales", "American sales"],
    "eu_sales": ["Europe sales", "EU revenue", "European sales", "EMEA sales"],
    "jp_sales": ["Japan sales", "JP revenue", "Japanese sales", "Asia sales"],
    "other_region_sales": ["miscellaneous sales", "other region sales", "additional sales", "ROW sales"],
    "total_global_sales": ["worldwide sales", "total sales", "global revenue", "all-region sales", "globalsales", "global sales"],
    "transaction_id": ["transaction ID", "order number", "receipt number", "purchase ID"],
    "product_name": ["item name", "product name", "food item", "menu item", "dish"],
    "product_category": ["foodtype", "category", "food type", "cuisine type", "menu category", "itemtype"],
    "unit_price": ["item price", "food price", "price per item", "unit price", "cost of item", "rate"],
    "item_count": ["number of items", "count", "item quantity"],
    "total_amount": ["total amount", "order value", "bill amount", "payment total"],
    "payment_method": ["payment method", "transaction mode", "billing type", "payment type", "transaction_type", "transactiontype"],
    "transaction_date": ["date", "dates", "transaction date", "day", "days", "transaction_date"],
}

# Mapping of operators remains unchanged
operator_mappings = {
    '=': '=',
    '==': '=',
    '>': '>',
    '<': '<',
    '>=': '>=',
    '<=': '<=',
    '!=': '!=',
    'equal to': '=',
    'equals': '=',
    'greater than or equal to': '>=',
    'less than or equal to': '<=',
    'not equal to': '!=',
    'not equals': '!=',
    'greater than': '>',
    'less than': '<',
    'fewer than': '<',
    'more than': '>',
    'greater': '>',
    'exceeds': '>',
    'less': '<',
    'above': '>',
    'below': '<',
    'is': '='
}

# SQL patterns with an added pattern for HAVING clause
sql_patterns = {
    "aggregate <A> by <B>": """SELECT {B}, {aggregate_func}({A}) AS {alias}
FROM {table}
GROUP BY {B}{order_clause}{limit_clause};""",

    "aggregate <A> by <B> having <condition>": """SELECT {B}, {aggregate_func}({A}) AS {alias}
FROM {table}
GROUP BY {B}
HAVING {aggregate_func}({A}) {op} {value}{order_clause}{limit_clause};""",

    "retrieve <A> where <B>": """SELECT {A}
FROM {table}
WHERE {B} {op} {value}{order_clause}{limit_clause};""",

    "retrieve <A>": """SELECT {A}
FROM {table}{order_clause}{limit_clause};""",
}

# Synonyms and related terms
aggregation_terms = ["total", "sum", "calculate", "compute", "aggregate", "add up", "summarize"]
retrieval_terms = ["list", "show", "display", "retrieve", "get", "fetch"]
sorting_terms = ["order by", "ordered by", "sort by", "sorted by", "arranged by", "based on", "highest", "lowest"]
sorting_directions = ["ascending", "asc", "descending", "desc", "highest number", "ascending order", "descending order"]
limiting_terms = ["top", "first", "highest", "most", "bottom", "last", "lowest", "least", "worst"]

# Aggregate functions
aggregate_funcs = ["sum", "average", "count", "min", "max"]

# Mapping of aggregate functions to their SQL equivalents
aggregate_func_mapping = {
    "SUM": "SUM",
    "AVERAGE": "AVG",
    "COUNT": "COUNT",
    "MIN": "MIN",
    "MAX": "MAX"
}

# Synonym mapping for aggregate functions
agg_synonym_map = {
    'average': 'avg',
    'sum': 'sum',
    'total': 'sum',
    'count': 'count',
    'minimum': 'min',
    'maximum': 'max',
    'min': 'min',
    'max': 'max'
}

# List of words to exclude
excluded_terms = ["of", "and", "in", "on", "at", "by", "for"]

# Name of the database table
database_table = "vgsales1"



def find_matching_column(search_term, schema_map):
    search_term = search_term.lower()
    for column_key, synonyms in schema_map.items():
        if search_term == column_key.lower() or search_term in [syn.lower() for syn in synonyms]:
            return column_key
    # Singularize if necessary
    if search_term.endswith('es'):
        singular_term = search_term[:-2]
    elif search_term.endswith('s'):
        singular_term = search_term[:-1]
    else:
        singular_term = search_term
    if singular_term != search_term:
        for column_key, synonyms in schema_map.items():
            if singular_term == column_key.lower() or singular_term in [syn.lower() for syn in synonyms]:
                return column_key
    return None


def find_operator(input_terms_list):
    for op_length in range(5, 0, -1):
        for index in range(len(input_terms_list) - op_length + 1):
            potential_op = ' '.join(input_terms_list[index:index + op_length])
            op_symbol = operator_mappings.get(potential_op)
            if op_symbol:
                return op_symbol, op_length
    return None, 0


def process_input_text(input_text, database_table):
    # Ensure NLTK stopwords are downloaded
    nltk.download('stopwords', quiet=True)
    from nltk.corpus import stopwords

    # Define filtered stop words, excluding essential keywords
    filtered_stop_words = set(stopwords.words('english'))
    essential_keywords = {'retrieve', 'aggregate', 'where', 'having', 'by', 'order', 'group', 'select', 'top', 'bottom', 'on', 'a', 'than', 'and', 'to', 'not', 'or', 'of', 'is'}
    filtered_stop_words = filtered_stop_words - essential_keywords
    filtered_stop_words.add('data')
    
    # Remove unnecessary punctuation (keep operator symbols, slashes, quotes, and commas)
    operator_symbols = set(['=', '>', '<', '!', '%'])
    punctuation_exceptions = set(['/', "'", ','])
    punctuation_to_strip = ''.join(c for c in string.punctuation if c not in operator_symbols and c not in punctuation_exceptions)
    input_text = input_text.translate(str.maketrans("", "", punctuation_to_strip))
    input_text = input_text.rstrip('.')  # Remove period at the end if present

    # Preserve the original text for accurate extraction
    original_text = input_text

    # Convert text to lowercase for uniformity
    input_text = input_text.lower()

    # Remove stop words from the text
    tokens = input_text.split()
    tokens = [word for word in tokens if word not in filtered_stop_words]
    input_text = ' '.join(tokens)

    # Combine aggregation terms and aggregate functions
    aggregation_terms_combined = aggregation_terms + aggregate_funcs

    # Initialize ordering and limiting placeholders
    raw_order_column = None
    sort_direction = ''
    sorted_column = None
    limit_number = None

    # Check for limiting phrases in text
    limit_found = None
    for synonym in limiting_terms:
        pattern = r'\b' + re.escape(synonym) + r'\s+(\d+)\b'
        match = re.search(pattern, input_text)
        if match:
            limit_found = match.group()
            limit_number = match.group(1)
            input_text = input_text.replace(limit_found, '').strip()
            # Determine sort direction based on synonym
            if synonym in ['top', 'first', 'highest', 'most']:
                sort_direction = 'desc'
            elif synonym in ['bottom', 'last', 'lowest', 'least', 'worst']:
                sort_direction = 'asc'
            break

    # Check for sorting phrases in text
    sorting_match = None
    for phrase in sorting_terms:
        if phrase in input_text:
            sorting_match = phrase
            break

    if sorting_match:
        sorting_index = input_text.find(sorting_match)
        # Extract sorting column and direction
        sorting_part = input_text[sorting_index + len(sorting_match):].strip()
        # Remove sorting part from text
        input_text = input_text[:sorting_index].strip()
        # Check if direction is specified
        for direction in sorting_directions:
            if sorting_part.endswith(' ' + direction):
                sort_direction = direction
                sorting_part = sorting_part[: -len(direction)].strip()
                break
        else:
            if not sort_direction:
                sort_direction = 'asc'  # Default to ascending if not set by limit synonym
        raw_order_column = sorting_part.strip()

    # Now, split the text into words after processing limit and sorting
    words = input_text.lower().split()

    # Check for aggregation terms or retrieve terms
    aggregation_word = next((word for word in words if word in aggregation_terms_combined), None)
    retrieval_word = next((word for word in retrieval_terms), None)

    # Initialize limit clause
    if limit_number:
        limit_clause = f"\nLIMIT {limit_number}"
    else:
        limit_clause = ''

    # Proceed with parsing logic, adding limit clause where appropriate
    # For 'aggregate' patterns
    if aggregation_word and "by" in words:
        aggregate_index = words.index(aggregation_word)
        by_index = words.index("by")

        # Check if 'having' is present
        if "having" in words:
            having_index = words.index("having")
            if aggregate_index + 1 >= by_index or by_index + 1 >= having_index:
                print(f"Error: Missing or invalid columns between 'aggregate', 'by', and 'having'.")
                return None, {}
            # Extract A, B, and condition
            a_raw_tokens = words[aggregate_index + 1:by_index]
            b_raw_tokens = words[by_index + 1:having_index]
            condition_tokens = words[having_index + 1:]

            # Remove stop words from tokens
            a_raw_tokens = [token for token in a_raw_tokens if token not in filtered_stop_words]
            b_raw_tokens = [token for token in b_raw_tokens if token not in filtered_stop_words]
            condition_tokens = [token for token in condition_tokens if token not in filtered_stop_words]

            a_raw = ' '.join(a_raw_tokens)
            b_raw = ' '.join(b_raw_tokens)
            print(f"Raw inputs: A tokens='{a_raw_tokens}', B tokens='{b_raw_tokens}', condition='{condition_tokens}'")

            # Determine the aggregate function
            if aggregation_word in aggregate_funcs:
                aggregate_function = aggregate_func_mapping.get(aggregation_word.upper(), aggregation_word.upper())
            else:
                aggregate_function = "SUM"  # Default aggregate function

            # Check if an aggregate function is specified in a_raw_tokens
            for func in aggregate_funcs:
                if func in a_raw_tokens:
                    aggregate_function = aggregate_func_mapping.get(func.upper(), func.upper())
                    func_index = a_raw_tokens.index(func)
                    # Assume the rest after the function is the column
                    a_raw_tokens = a_raw_tokens[func_index + 1:]
                    a_raw = ' '.join(a_raw_tokens)
                    break

            print(f"Aggregate function: {aggregate_function}")
            print(f"Column A after extracting aggregate function and removing stop words: '{a_raw}'")

            # Match terms to schema structure
            a = find_matching_column(a_raw.strip(), schema_structure)
            b = find_matching_column(b_raw.strip(), schema_structure)
            print(f"Matched columns: A='{a}', B='{b}'")
            if not a or not b:
                print(f"Error: Unable to match columns for 'aggregate <A> by <B>'. A={a}, B={b}")
                return None, {}  # Invalid columns

            # Parse condition
            condition_str = ' '.join(condition_tokens)
            print(f"Condition string: '{condition_str}'")

            # Initialize variables
            operator_symbol = None
            condition_value = None
            condition_lhs = None

            # Try to find the operator within the condition string
            for op_phrase in sorted(operator_mappings.keys(), key=lambda x: -len(x.split())):
                if f' {op_phrase} ' in f' {condition_str} ':
                    operator_symbol = operator_mappings[op_phrase]
                    # Split the condition string using the operator
                    parts = condition_str.split(op_phrase)
                    if len(parts) == 2:
                        condition_lhs = parts[0].strip()
                        condition_value = parts[1].strip()
                    break

            if not operator_symbol or not condition_value:
                print("Error: Invalid condition in HAVING clause.")
                return None, {}

            print(f"Parsed condition: lhs='{condition_lhs}', operator='{operator_symbol}', value='{condition_value}'")

            # Handle value quoting
            try:
                float(condition_value)
                # It's a number, no need to modify
            except ValueError:
                # Not a number, ensure it's properly quoted
                condition_value = condition_value.strip("'\"")  # Remove existing quotes
                condition_value = f"'{condition_value}'"

            # Prepare alias for the aggregated column
            alias = a_raw.strip().replace(' ', '_')

            # Handle sorting for aggregate queries
            if raw_order_column:
                sorted_column = find_matching_column(raw_order_column.strip(), schema_structure)
                if not sorted_column:
                    # Maybe the sorting is on the alias
                    sorted_column_normalized = raw_order_column.lower().replace(' ', '_')
                    possible_alias = f"{aggregate_function.lower()}_{alias}"
                    if sorted_column_normalized == possible_alias.lower():
                        sorted_column = possible_alias
                    else:
                        print(f"Error: Unable to match sorting column '{raw_order_column}'.")
                        return None, {}
                # Determine sort direction
                if sort_direction.lower() in ['descending', 'desc', 'highest', 'descending order']:
                    sort_dir_sql = 'DESC'
                else:
                    sort_dir_sql = 'ASC'
                order_clause = f"\nORDER BY {aggregate_function.lower()}_{alias} {sort_dir_sql}"
            else:
                order_clause = ''

            # Define placeholders before adding limit_clause
            placeholders = {
                "A": a,
                "B": b,
                "aggregate_function": aggregate_function,
                "aggregate_function_lower": aggregate_function.lower(),
                "table": database_table,
                "alias": f"{aggregate_function.lower()}_{alias}",
                "op": operator_symbol,
                "value": condition_value,
                "order_clause": order_clause,
                "limit_clause": limit_clause  # Include limit_clause here
            }

            return "aggregate <A> by <B> having <condition>", placeholders

        else:
            # Existing 'aggregate <A> by <B>' pattern
            if aggregate_index + 1 >= by_index:
                print(f"Error: Missing or invalid column between '{aggregation_word}' and 'by'.")
                return None, {}
            # Extract all words between aggregation term and 'by' for A
            a_raw_tokens = words[aggregate_index + 1:by_index]
            # Extract all words after 'by' for B
            b_raw_tokens = words[by_index + 1:]

            # Remove stop words from tokens
            a_raw_tokens = [token for token in a_raw_tokens if token not in filtered_stop_words]
            b_raw_tokens = [token for token in b_raw_tokens if token not in filtered_stop_words]

            a_raw = ' '.join(a_raw_tokens)
            b_raw = ' '.join(b_raw_tokens)
            print(f"Raw inputs: A tokens='{a_raw_tokens}', B tokens='{b_raw_tokens}'")

            # Determine the aggregate function
            if aggregation_word in aggregate_funcs:
                aggregate_function = aggregate_func_mapping.get(aggregation_word.upper(), aggregation_word.upper())
            else:
                aggregate_function = "SUM"  # Default aggregate function

            # Check if an aggregate function is specified in a_raw_tokens
            for func in aggregate_funcs:
                if func in a_raw_tokens:
                    aggregate_function = aggregate_func_mapping.get(func.upper(), func.upper())
                    func_index = a_raw_tokens.index(func)
                    # Assume the rest after the function is the column
                    a_raw_tokens = a_raw_tokens[func_index + 1:]
                    a_raw = ' '.join(a_raw_tokens)
                    break

            print(f"Aggregate function: {aggregate_function}")
            print(f"Column A after extracting aggregate function and removing stop words: '{a_raw}'")

            # Match terms to schema structure
            a = find_matching_column(a_raw.strip(), schema_structure)
            b = find_matching_column(b_raw.strip(), schema_structure)
            print(f"Matched columns: A='{a}', B='{b}'")
            if not a or not b:
                print(f"Error: Unable to match columns for 'aggregate <A> by <B>'. A={a}, B={b}")
                return None, {}  # Invalid columns

            # Prepare alias for the aggregated column
            alias = a_raw.strip().replace(' ', '_')

            # Handle sorting for aggregate queries
            if raw_order_column:
                sorted_column = find_matching_column(raw_order_column.strip(), schema_structure)
                if not sorted_column:
                    # Maybe the sorting is on the alias
                    sorted_column_normalized = raw_order_column.lower().replace(' ', '_')
                    possible_alias = f"{aggregate_function.lower()}_{alias}"
                    print("sorted_column_normalized", sorted_column_normalized)
                    print("possible_alias", possible_alias)
                    if sorted_column_normalized == possible_alias.lower():
                        sorted_column = possible_alias
                    else:
                        # print(f"Error: Unable to match sorting column '{raw_order_column}'.")
                        # return None, {}
                        print("Unable to match sorting column; defaulting to possible alias.")
                        sorted_column = possible_alias
                # Determine sort direction
                if sort_direction.lower() in ['descending', 'desc', 'highest', 'descending order']:
                    sort_dir_sql = 'DESC'
                else:
                    sort_dir_sql = 'ASC'
                order_clause = f"\nORDER BY {aggregate_function.lower()}_{alias} {sort_dir_sql}"
            else:
                # No sorting specified
                order_clause = ''

            placeholders = {
                "A": a,
                "B": b,
                "aggregate_function": aggregate_function,
                "aggregate_function_lower": aggregate_function.lower(),
                "table": database_table,
                "alias": f"{aggregate_function.lower()}_{alias}",
                "order_clause": order_clause,
                "limit_clause": limit_clause  # Include limit_clause here
            }
            return "aggregate <A> by <B>", placeholders

    elif retrieval_word:
        retrieve_indices = [i for i, word in enumerate(words) if word == retrieval_word]
        where_indices = [i for i, word in enumerate(words) if word == "where"]

        # Determine if 'where' is present
        if where_indices:
            # Handle 'retrieve <A> where <B>' pattern
            retrieve_index = retrieve_indices[0]
            where_index = where_indices[0]

            # Extract all words between 'retrieve' and 'where' for A
            a_raw_tokens = words[retrieve_index + 1:where_index]
            # Do not remove stop words here to preserve 'and'

            if not a_raw_tokens:
                # No columns specified, default to '*'
                a = '*'
            else:
                # Combine tokens
                a_raw = ' '.join(a_raw_tokens)
                # Insert spaces around commas
                a_raw = re.sub(r',', ' , ', a_raw)
                # Split columns on commas and 'and'
                columns = re.split(r'\s*(?:,|and)\s*', a_raw)
                column_names = []
                for col in columns:
                    col = col.strip()
                    if not col:
                        continue
                    # Optionally remove stop words from individual column names
                    col_tokens = [token for token in col.split() if token not in filtered_stop_words]
                    col = ' '.join(col_tokens)
                    column = find_matching_column(col, schema_structure)
                    if column:
                        column_names.append(column)
                    else:
                        print(f"Error: Unable to match column '{col}'.")
                        return None, {}
                a = ', '.join(column_names)

            # Extract condition after 'where'
            condition_tokens = words[where_index + 1:]
            if not condition_tokens:
                print("Error: Incomplete condition after 'where'.")
                return None, {}
            condition_str = ' '.join(condition_tokens)

            # Try to find the operator within the condition string
            operator_symbol = None
            condition_value = None
            condition_column_raw = None
            for op_phrase in sorted(operator_mappings.keys(), key=lambda x: -len(x.split())):
                if f' {op_phrase} ' in f' {condition_str} ':
                    operator_symbol = operator_mappings[op_phrase]
                    parts = condition_str.split(op_phrase)
                    if len(parts) == 2:
                        condition_column_raw = parts[0].strip()
                        condition_value = parts[1].strip()
                    break

            if not operator_symbol or not condition_value:
                print("Error: Invalid condition in WHERE clause.")
                return None, {}

            # Match condition column
            condition_column = find_matching_column(condition_column_raw, schema_structure)
            if not condition_column:
                print(f"Error: Unable to match condition column '{condition_column_raw}'.")
                return None, {}

            # Handle cases where 'top' or 'highest' is used without an explicit sorting column
            if limit_number and not sorting_match and any(agg_word in condition_column_raw for agg_word in ['total', 'sum', 'average', 'count', 'max', 'min']):
                # Treat as an aggregate query with a HAVING clause
                aggregate_function = 'SUM'  # Default to SUM; can enhance based on condition_column_raw
                # Use aggregation_word if present
                if aggregation_word:
                    aggregate_function = agg_synonym_map.get(aggregation_word.lower(), 'SUM').upper()
                # Extract B (grouping column) from A
                b_raw = a if a != '*' else 'store'  # Default grouping column if not specified
                b = find_matching_column(b_raw.strip(), schema_structure)
                if not b:
                    print(f"Error: Unable to match grouping column '{b_raw}'.")
                    return None, {}
                a_column = condition_column  # The column to aggregate (e.g., 'sales')
                alias = f"{aggregate_function.lower()}_{a_column.replace('.', '_')}"
                # Handle sorting
                sorted_column = f"{aggregate_function}({a_column})"
                if sort_direction.lower() in ['descending', 'desc']:
                    sort_dir_sql = 'DESC'
                else:
                    sort_dir_sql = 'ASC'
                order_clause = f"\nORDER BY {sorted_column} {sort_dir_sql}"
                # Handle value quoting
                try:
                    float(condition_value)
                except ValueError:
                    condition_value = condition_value.strip("'\"")
                    condition_value = f"'{condition_value}'"
                placeholders = {
                    "A": a_column,
                    "B": b,
                    "aggregate_function": aggregate_function,
                    "alias": alias,
                    "table": database_table,
                    "op": operator_symbol,
                    "value": condition_value,
                    "order_clause": order_clause,
                    "limit_clause": limit_clause
                }
                return "aggregate <A> by <B> having <condition>", placeholders
            else:
                # Proceed as normal retrieve query
                # Handle sorting for retrieve queries
                if raw_order_column:
                    sorted_column = find_matching_column(raw_order_column.strip(), schema_structure)
                    if not sorted_column:
                        print(f"Error: Unable to match sorting column '{raw_order_column}'.")
                        return None, {}
                else:
                    # Default to sorting by the condition column if limit is specified
                    if limit_number:
                        sorted_column = condition_column
                    else:
                        sorted_column = None

                if sorted_column:
                    # Determine sort direction
                    if sort_direction.lower() in ['descending', 'desc', 'highest', 'descending order']:
                        sort_dir_sql = 'DESC'
                    else:
                        sort_dir_sql = 'ASC'
                    order_clause = f"\nORDER BY {sorted_column} {sort_dir_sql}"
                else:
                    order_clause = ''

                # Handle value quoting
                try:
                    float(condition_value)
                except ValueError:
                    condition_value = condition_value.strip("'\"")
                    condition_value = f"'{condition_value}'"
                placeholders = {
                    "A": a,
                    "B": condition_column,
                    "op": operator_symbol,
                    "value": condition_value,
                    "table": database_table,
                    "order_clause": order_clause,
                    "limit_clause": limit_clause
                }
                return "retrieve <A> where <B>", placeholders
        else:
            # Handle 'retrieve <A>' pattern without 'where'
            retrieve_index = retrieve_indices[0]
            # Extract all words after 'retrieve' for A
            a_raw_tokens = words[retrieve_index + 1:]
            # Do not remove stop words here to preserve 'and'

            if not a_raw_tokens:
                # No columns specified, default to '*'
                a = '*'
            else:
                # Combine tokens
                a_raw = ' '.join(a_raw_tokens)
                # Insert spaces around commas
                a_raw = re.sub(r',', ' , ', a_raw)
                # Split columns on commas and 'and'
                columns = re.split(r'\s*(?:,|and)\s*', a_raw)
                column_names = []
                for col in columns:
                    col = col.strip()
                    if not col:
                        continue
                    # Optionally remove stop words from individual column names
                    col_tokens = [token for token in col.split() if token not in filtered_stop_words]
                    col = ' '.join(col_tokens)
                    column = find_matching_column(col, schema_structure)
                    if column:
                        column_names.append(column)
                    else:
                        print(f"Error: Unable to match column '{col}'.")
                        return None, {}
                a = ', '.join(column_names)

            # Handle sorting for retrieve queries
            if raw_order_column:
                sorted_column = find_matching_column(raw_order_column.strip(), schema_structure)
                if not sorted_column:
                    print(f"Error: Unable to match sorting column '{raw_order_column}'.")
                    return None, {}
                # Determine sort direction
                if sort_direction.lower() in ['descending', 'desc', 'highest', 'descending order']:
                    sort_dir_sql = 'DESC'
                else:
                    sort_dir_sql = 'ASC'
                order_clause = f"\nORDER BY {sorted_column} {sort_dir_sql}"
            else:
                order_clause = ''

            placeholders = {
                "A": a,
                "table": database_table,
                "order_clause": order_clause,
                "limit_clause": limit_clause
            }
            return "retrieve <A>", placeholders

    else:
        print("Pattern not recognized in input text.")
        return None, {}

def create_sql_query(query_pattern_key, sql_placeholders):
    """
    Generate the SQL query based on the pattern and placeholders.
    """
    if query_pattern_key in sql_patterns:
        query_template = sql_patterns[query_pattern_key]

        # Generate the query
        sql_query = query_template.format(**sql_placeholders)
        return sql_query
    print("Pattern not recognized.")
    return "Pattern not recognized."


def map_to_column(text: str) -> str:
    ignore_list = ["select", "get", "find", "fetch", "count", "number", "of", "and", ",", "order", "sort", "by", "ascending", "descending", "asc", "desc", "group", "made"]
    if text.lower() in ignore_list:
        return None
    return column_mappings.get(text.lower(), text)

# Helper function to map natural language operators to SQL
def map_operator(op: str) -> str:
    mapping = {
        "greater than": ">",
        "great than": ">",
        "less than": "<",
        "equals": "=",
        "equal to": "=",
        "greater or equal": ">=",
        "less or equal": "<=",
        "greater": ">",
        "less": "<",
        ">": ">",
        "<": "<",
        "=": "=",
        ">=": ">=",
        "<=": "<="
    }
    return mapping.get(op.lower(), "=")

# Helper function for Aggregate Function Mapping
def map_aggregate(tokens: list) -> str:
    if "average" in tokens or "avg" in tokens:
        return "AVG"
    elif "sum" in tokens or "total" in tokens:
        return "SUM"
    elif "minimum" in tokens or "min" in tokens:
        return "MIN"
    elif "maximum" in tokens or "max" in tokens:
        return "MAX"
    elif "count" in tokens:
        return "COUNT"
    return None

# Function to parse and generate SQL query
def parse_and_generate_sql(user_query: str) -> str:
    doc = nlp(user_query)
    matches = matcher(doc)

    # Initialize query parts
    query_parts = {
        "SELECT": [],
        "WHERE": [],
        "ORDER_BY": "",
        "GROUP_BY": "",
        "HAVING": []
    }

    # Process matches
    for match_id, start, end in matches:
        intent = nlp.vocab.strings[match_id]
        span = doc[start:end]

        if intent in ["SELECT", "SELECT_MULTIPLE"]:
            for token in span:
                column = map_to_column(token.text)
                if column and column not in query_parts["SELECT"]:
                    query_parts["SELECT"].append(column)

        elif intent == "WHERE":
            tokens = [token.text.lower() for token in span]
            if len(tokens) >= 4:
                column = map_to_column(tokens[1])
                operator = map_operator(tokens[2])
                value = tokens[3].capitalize() if tokens[3].isalpha() else tokens[3]
                if column:
                    condition = f"{column} {operator} '{value}'" if operator == "=" else f"{column} {operator} {value}"
                    query_parts["WHERE"].append(condition)

        elif intent == "WHERE_MADE_BY":
            tokens = [token.text.lower() for token in span]
            if len(tokens) >= 3:
                column = "make"
                operator = "="
                value_tokens = tokens[2:]  # In case manufacturer name consists of multiple words
                value = ' '.join([token.capitalize() for token in value_tokens])
                condition = f"{column} {operator} '{value}'"
                query_parts["WHERE"].append(condition)

        elif intent == "WHERE_YEAR":
            tokens = [token.text.lower() for token in span]
            if len(tokens) >= 2:
                column = "year"
                operator = "="
                value = tokens[-1]
                condition = f"{column} {operator} {value}"
                query_parts["WHERE"].append(condition)

        elif intent == "AGGREGATE":
            tokens = [token.text.lower() for token in span]
            if len(tokens) >= 2:
                agg_func = map_aggregate(tokens)
                column = map_to_column(tokens[-1])
                if column and agg_func:
                    agg_select = f"{agg_func}({column})"
                    if agg_select not in query_parts["SELECT"]:
                        query_parts["SELECT"].append(agg_select)

        elif intent == "COUNT":
            query_parts["SELECT"].append("COUNT(*)")

        elif intent in ["ORDER_BY", "SORT"]:
            tokens = [token.text.lower() for token in span]
            if len(tokens) >= 3:
                column = map_to_column(tokens[-2] if len(tokens) > 3 else tokens[-1])
                if column:
                    direction = "ASC" if tokens[-1] in ["ascending", "asc"] else "DESC"
                    query_parts["ORDER_BY"] = f"{column} {direction}"

        elif intent == "GROUP_BY":
            tokens = [token.text.lower() for token in span]
            group_by_columns = [map_to_column(token) for token in tokens if map_to_column(token)]
            query_parts["GROUP_BY"] = ", ".join(group_by_columns)
            # Ensure group_by_columns are in SELECT
            for column in group_by_columns:
                if column and column not in query_parts["SELECT"]:
                    query_parts["SELECT"].append(column)

        elif intent == "HAVING":
            tokens = [token.text.lower() for token in span]
            # Find aggregate function
            agg_func = map_aggregate(tokens)
            # Find index of agg_func in tokens
            agg_indices = [i for i, token in enumerate(tokens) if token in ['sum', 'average', 'avg', 'count', 'minimum', 'min', 'maximum', 'max']]
            if agg_indices:
                agg_index = agg_indices[0]
                # Column is token(s) after agg_func up to operator
                operator_tokens_set = {"greater than", "less than", "greater or equal", "less or equal", "equals", "equal to", "greater", "less", "than", ">", "<", "="}
                column_tokens = []
                idx = agg_index + 1
                while idx < len(tokens):
                    op_candidate = tokens[idx]
                    if op_candidate in operator_tokens_set:
                        break
                    column_tokens.append(tokens[idx])
                    idx += 1
                column = ' '.join(column_tokens).strip()
                if not column and agg_func == 'COUNT':
                    column = '*'
                else:
                    column = map_to_column(column)
                # Now collect operator tokens
                operator = None
                operator_tokens_list = []
                while idx < len(tokens) and len(operator_tokens_list) < 2:
                    operator_tokens_list.append(tokens[idx])
                    op_candidate = ' '.join(operator_tokens_list)
                    if op_candidate in operator_tokens_set:
                        operator = map_operator(op_candidate)
                        idx += 1
                        break
                    idx += 1
                if not operator and idx < len(tokens):
                    operator = map_operator(tokens[idx - 1])
                # Get value
                if idx < len(tokens):
                    value = tokens[idx]
                else:
                    value = None
                if agg_func and column and operator and value:
                    condition = f"{agg_func}({column}) {operator} {value}"
                    if condition not in query_parts["HAVING"]:
                        query_parts["HAVING"].append(condition)
                    # Ensure the aggregate function is in SELECT
                    agg_select = f"{agg_func}({column})"
                    if agg_select not in query_parts["SELECT"]:
                        query_parts["SELECT"].append(agg_select)

    # Ensure no duplicate SELECT columns
    query_parts["SELECT"] = list(set(query_parts["SELECT"]))

    # Build SELECT clause
    select_clause = ", ".join(query_parts["SELECT"]) if query_parts["SELECT"] else "*"

    # Build WHERE and HAVING clauses
    where_clause = " AND ".join(query_parts["WHERE"])
    having_clause = " AND ".join(query_parts["HAVING"])

    # Generate SQL query
    sql = f"SELECT {select_clause} FROM vehicles"
    if where_clause:
        sql += f" WHERE {where_clause}"
    if query_parts["GROUP_BY"]:
        sql += f" GROUP BY {query_parts['GROUP_BY']}"
    if having_clause:
        sql += f" HAVING {having_clause}"
    if query_parts["ORDER_BY"]:
        sql += f" ORDER BY {query_parts['ORDER_BY']}"

    return sql
