import spacy
from spacy.matcher import Matcher
from typing import Dict

# Load SpaCy model
nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)

# Define SQL patterns
patterns = {
    "SELECT": [
        {"LOWER": {"IN": ["select", "get", "find", "fetch"]}}, 
        {"POS": "NOUN", "OP": "+"}
    ],
    "SELECT_MULTIPLE": [
        {"POS": "NOUN", "OP": "+"},
        {"LOWER": {"IN": [",", "and"]}, "OP": "?"},
        {"POS": "NOUN", "OP": "+"}
    ],
    "WHERE": [
        {"LOWER": {"IN": ["where", "made"]}}, 
        {"POS": "NOUN", "OP": "+"}, 
        {"LOWER": {"IN": ["by", "is", "="]}}, 
        {"POS": {"IN": ["PROPN", "NOUN", "NUM"]}}
    ],
    "WHERE_YEAR": [
        {"LOWER": {"IN": ["manufactured", "in", "made", "where"]}}, 
        {"LOWER": {"IN": ["year"]}, "OP": "?"}, 
        {"POS": "NUM"}
    ],
    "AGGREGATE": [
        {"LOWER": {"IN": ["total", "sum", "average", "avg", "count", "minimum", "min", "maximum", "max"]}}, 
        {"POS": "NOUN", "OP": "+"}
    ],
    "COUNT": [
        {"LOWER": {"IN": ["count", "number"]}}, 
        {"LOWER": {"IN": ["of"]}, "OP": "?"}, 
        {"POS": "NOUN", "OP": "+"}
    ],
    "ORDER_BY": [
        {"LOWER": {"IN": ["order"]}},
        {"LOWER": "by"},
        {"POS": "NOUN"},
        {"LOWER": {"IN": ["ascending", "descending", "asc", "desc"]}, "OP": "?"}
    ],
    "SORT": [
        {"LOWER": "sort"},
        {"POS": "NOUN", "OP": "+"},
        {"LOWER": "by"},
        {"POS": "NOUN"},
        {"LOWER": {"IN": ["ascending", "descending", "asc", "desc"]}, "OP": "?"}
    ],
     "GROUP_BY": [
        {"LOWER": {"IN": ["group"]}},
        {"LOWER": "by"},
        {"POS": "NOUN", "OP": "+"},
        {"LOWER": "and", "OP": "?"},
        {"POS": "NOUN", "OP": "*"}
    ],
   "HAVING": [
    {"LOWER": "having"},
    {"LOWER": {"IN": ["sum", "avg", "average", "count", "minimum", "min", "maximum", "max"]}, "OP": "?"},  # Optional aggregate function
    {"POS": "NOUN", "OP": "+"},  # Column name
    {"LOWER": {"IN": ["greater","greater than","less than", "less", "equals", "than", ">", "<", "="]}, "OP": "+"},  # Operator or natural language equivalent
    {"POS": {"IN": ["NUM", "NOUN"]}}  # Value
]

}

# Add patterns to the matcher
for intent, pattern in patterns.items():
    matcher.add(intent, [pattern])

# Define column mappings
column_mappings = {
    "make": "make",
    "model": "model",
    "year": "year",
    "condition": "condition",
    "mileage": "mileage",
    "price": "price",  # Added price column
    "vehicles": None,
    "cars": None,
    "details": None,  # Generic terms to be ignored
}

# Helper function to map column names, excluding action words
def map_to_column(text: str) -> str:
    if text.lower() in ["select", "get", "find", "fetch", "count", "number", "of", "and", ",", "order", "sort", "by", "ascending", "descending", "asc", "desc","group"]:
        return None
    return column_mappings.get(text.lower(), text)
    
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

    # Helper function to map natural language operators to SQL
    def map_operator(op: str) -> str:
        mapping = {
            "greater than": ">",
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

    # Helper function to map phrases to aggregate functions
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
                agg_func = tokens[0].upper()
                column = map_to_column(tokens[1])
                if column:
                    query_parts["SELECT"].append(f"{agg_func}({column})")

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

        elif intent == "HAVING":
            tokens = [token.text.lower() for token in span]
            agg_func = map_aggregate(tokens)
            column = map_to_column(tokens[1]) if len(tokens) > 1 else None
            operator = map_operator(tokens[-2])
            value = tokens[-1]

            if agg_func and column:
                query_parts["HAVING"].append(f"{agg_func}({column}) {operator} {value}")
            elif agg_func:
                query_parts["HAVING"].append(f"{agg_func}(*) {operator} {value}")

    # Handle "made by" conditions for make
    for token in doc:
        if token.text.lower() == "made" and token.nbor(1).text.lower() == "by":
            make_value = token.nbor(2).text.capitalize()
            query_parts["WHERE"].append(f"make = '{make_value}'")

    # Ensure aggregation is used with GROUP BY if applicable
    if query_parts["HAVING"] and query_parts["GROUP_BY"]:
        if "COUNT(*)" in query_parts["SELECT"]:
            query_parts["SELECT"].insert(0, "COUNT(*)")

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


test_queries = [
    "get  mileage group by year having average mileage greater 1000",
    "get vehicle group by year having average condition > 4",
    "get  mileage group by year having average mileage = 1000",
    "group by condition and get the average price having average price greater 5000",
    "group by condition and get the average price having average price less 5000",
     "get total mileage group by year",
    "group by make and find the total price",
    "group by vehicles condition and get the average price",
    "find total cars group by year and condition",
    "get price from vehicles which is order by price",
    "find all details of cars made by toyota where year is 2020 order by price descending",
    "sort car by price ascending",
    "get cars made by Honda order by model descending",
    "sort vehicles by price ascending",
    "fetch model of car made by toyota",
    "fetch mileage of vehicle where year is 2022",
     "count the number of cars made by Toyota",
    "find year, condition, mileage and model of cars made by toyota where year is 2020",
    "find the condition of vehicles where year is 2022",
    "get condition of cars made by Toyota where year = 2019",
    "find the total mileage of cars made by toyota",
    "get the average condition of vehicles made by honda where year is 2020",
    "count the number of vehicles when year is 2021",
     "find year, condition, mileage and model of cars made by toyota where year is 2020",
    "find condition of vehicles where year is 2022",
    "get condition of cars made by Toyota where year = 2019",
    "find the total mileage of cars made by toyota",
    "get the average condition of vehicles made by honda where year is 2020",
    "count the number of cars made by Toyota",
    "find the maximum price of vehicles made by Toyota",
    "get the average price of cars where year is 2021",
    "find the total price of vehicles where year is 2020",
    "find the price, model of cars made by Ford",
    "find the maximum year of vehicles manufactured in 2021",
     "find  year, mileage of cars made by toyota where year is 2020",
    "Fetch model of vehicles made by toyota.",
    "Fetch details of vehicles manufactured in 2020.",
    "fetch  mileage of vehicle where year is 2022",
    "get all cars made by Toyota where year = 2019",
    "find manufacturer,model of vehicles manufactured in 2021",
    "fetch model of car made by toyota",
    "find the year, mileage of cars made by toyota where year = 2020",
    "get model of cars made by Toyota where in year 2019",
    "get maximum price of car made by Kia where model is Sorento"
]


for query in test_queries:
    sql_query = parse_and_generate_sql(query)
    print(f"User Query: {query}")
    print(f"SQL Query: {sql_query}\n")