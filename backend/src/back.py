import datetime
from flask import Flask, request, jsonify
import mysql.connector
from pymongo import MongoClient
import csv
from flask_cors import CORS
from bson.json_util import dumps
import re

import queryMapper
from queryMapper import parse_input, generate_query
import nosqlConvert;
from nosqlConvert import sql_to_mongodb
curr_table = ''
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Allow up to 16MB
CORS(app)

# Local MySQL Connection
mysql_conn = mysql.connector.connect(
    host="localhost", 
    user="root", 
    # password="Jesus&me2023!!", #dont read my password
    database="chatdb" 
)

# Local MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")  
mongo_db = mongo_client["chatdb_nosql"]  

# -----------------------SQL methods ------------------------
# # Convert NLQ to SQL
@app.route('/nlq-to-sql', methods=['POST'])
def nlq_to_sql():
    try:
        # Get the NLQ from the request
        nlq = request.json.get('nlq')
        if not nlq:
            return jsonify({"error": "No NLQ provided."}), 400

        # Convert NLQ to SQL using the mapper
        # sql_query = parse_and_generate_sql(nlq)  
        global curr_table
        pattern_key, placeholders = queryMapper.parse_input(nlq,curr_table)
        sql_query = queryMapper.generate_query(pattern_key, placeholders)

        # Return the SQL query
        return jsonify({"sql": sql_query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Execute Query in MySQL
@app.route('/query/mysql', methods=['POST'])
def execute_mysql_query():
    try:
        # Extract the query from the request
        query = request.json['query']
        print(f"Executing SQL query: {query}")  # Debug log

        # Execute the SQL query
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Serialize TIMESTAMP/DATETIME fields to string
        for row in results:
            for key, value in row.items():
                if isinstance(value, (datetime.datetime, datetime.date)):
                    row[key] = value.isoformat()  # Convert to ISO 8601 string

        print(f"Query results: {results}")  # Debug log
        return jsonify(results)  # Return the results as JSON
    except Exception as e:
        print(f"Error executing query: {e}")  # Log the error
        return jsonify({"error": str(e)}), 500
    
# Upload Dataset to MySQL
@app.route('/upload/mysql', methods=['POST'])
def upload_to_mysql():
    try:
        # Access the uploaded file
        file = request.files['file']
        table_name = request.form['table']
        print("table name in sql",table_name)
        # Decode the binary stream to text
        file_stream = file.stream.read().decode('utf-8')
        data = csv.reader(file_stream.splitlines())

        #current table that will be used to query -tan add
        global curr_table
        curr_table = table_name

        # Process the CSV data
        cursor = mysql_conn.cursor()
        columns = next(data)  # Get column headers

        # Wrap column names in backticks for MySQL
        column_definitions = ', '.join(f'`{col.strip()}` TEXT' for col in columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{curr_table}` ({column_definitions})"
        cursor.execute(create_table_query)

        # Insert rows into the table
        for row in data:
            values = ', '.join(f"'{v}'" for v in row)
            insert_query = f"INSERT INTO `{table_name}` VALUES ({values})"
            cursor.execute(insert_query)

        mysql_conn.commit()
        return jsonify({"message": f"Data uploaded {table_name} successfully to MySQL."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    # Fetch MySQL Metadata
@app.route('/metadata/mysql', methods=['GET'])
def get_mysql_metadata():
    try:
        cursor = mysql_conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        metadata = {}
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            metadata[table] = [row[0] for row in cursor.fetchall()]
        return jsonify(metadata)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
#Delete table from mySQL
@app.route('/delete/mysql', methods=['POST'])
def delete_mysql_table():
    try:
        table_name = request.json['table']

        # Check if the table exists
        cursor = mysql_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        mysql_conn.commit()

        return jsonify({"message": f"Table `{table_name}` deleted successfully (if it existed)."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# -----------------------MongoDB methods ------------------------


# @app.route('/sql-to-mongo-output', methods=['POST'])
# def sql_to_mongo_output():
#     try:
#         # Step 1: Get the SQL query from the request body
#         sql_query = request.json.get('sql_query')
#         if not sql_query:
#             return jsonify({"error": "SQL query not provided"}), 400

#         print(f"Received SQL Query: {sql_query}")

#         # Step 2: Convert SQL query to MongoDB query
#         mongodb_query = sql_to_mongodb(sql_query)
#         print(f"Generated MongoDB Query: {mongodb_query}")

#         # Step 3: Parse the MongoDB query for execution
#         import re
#         match = re.match(r"db\.(\w+)\.(\w+)\((.*)\)", mongodb_query.strip())
#         if not match:
#             return jsonify({"error": "Invalid MongoDB query format after conversion"}), 400

#         collection_name, method, args = match.groups()
#         print(f"Parsed collection: {collection_name}, method: {method}, args: {args}")

#         # Access the MongoDB collection
#         collection = mongo_db[collection_name]

#         # Safely evaluate the query arguments
#         from ast import literal_eval
#         try:
#             parsed_args = literal_eval(args) if args else {}
#         except Exception as e:
#             return jsonify({"error": f"Invalid query arguments: {str(e)}"}), 400

#         # Step 4: Execute the MongoDB query
#         if method == "find":
#             # Handle `find` queries
#             if isinstance(parsed_args, dict):
#                 results = collection.find(parsed_args)
#             elif isinstance(parsed_args, list) and len(parsed_args) == 2:
#                 results = collection.find(*parsed_args)  # Filter and projection
#             else:
#                 return jsonify({"error": "Invalid arguments for 'find'."}), 400
#         elif method == "aggregate":
#             # Handle `aggregate` queries
#             if isinstance(parsed_args, list):
#                 results = collection.aggregate(parsed_args)
#             else:
#                 return jsonify({"error": "Invalid arguments for 'aggregate'."}), 400
#         else:
#             return jsonify({"error": f"Unsupported method '{method}'."}), 400

#         # Step 5: Serialize and return the results as JSON
#         return jsonify({"data": json.loads(dumps(results))}), 200
#     except Exception as e:
#         print(f"Error: {e}")
#         return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# @app.route('/nlq-to-mongo', methods=['POST'])
# def nlq_to_mongo():
#     try:
#         # Step 1: Get the NLQ from the request
#         nlq = request.json.get('nlq')
#         if not nlq:
#             return jsonify({"error": "No NLQ provided."}), 400

#         print(f"Received NLQ: {nlq}")

#         # Step 2: Convert NLQ to SQL
#         global curr_table
#         pattern_key, placeholders = queryMapper.parse_input(nlq,curr_table)
#         sql_query = queryMapper.generate_query(pattern_key, placeholders)
#         if not sql_query:
#             return jsonify({"error": "Failed to generate SQL query."}), 500

#         # Debug SQL query before and after removing the semicolon
#         print(f"Generated SQL Query (raw): {sql_query}")
#         sql_query = sql_query.rstrip(';')
#         print(f"Generated SQL Query (cleaned): {sql_query}")

#         # Step 3: Convert SQL query to MongoDB query
#         mongodb_query = sql_to_mongodb(sql_query)
#         if not mongodb_query:
#             return jsonify({"error": "Failed to generate MongoDB query."}), 500

#         print(f"Generated MongoDB Query: {mongodb_query}")
#         print(f"Returning MongoDB Query: {mongodb_query}")

#         # Step 4: Parse the MongoDB query
#         import re
#         match = re.match(r"db\.(\w+)\.(\w+)\((.*)\)", mongodb_query.strip())
#         if not match:
#             return jsonify({"error": "Invalid MongoDB query format after conversion"}), 400

#         collection_name, method, args = match.groups()
#         print(f"Parsed MongoDB - Collection: {collection_name}, Method: {method}, Args: {args}")

#         # Step 5: Access the MongoDB collection
#         collection = mongo_db[collection_name]
#         print(collection)
#         # Safely evaluate the query arguments
#         from ast import literal_eval
#         try:
#             parsed_args = literal_eval(args) if args else {}
#             print(f"Parsed Arguments for MongoDB Query: {parsed_args}")
#         except Exception as e:
#             return jsonify({"error": f"Invalid query arguments: {str(e)}"}), 400

#         # Step 6: Execute the MongoDB query
#         if method == "find":
#             # Handle `find` queries
#             if isinstance(parsed_args, dict):
#                 results = collection.find(parsed_args)
#             elif isinstance(parsed_args, list) and len(parsed_args) == 2:
#                 results = collection.find(*parsed_args)  # Filter and projection
#             else:
#                 return jsonify({"error": "Invalid arguments for 'find'."}), 400
#         elif method == "aggregate":
#             # Handle `aggregate` queries
#             if isinstance(parsed_args, list):
#                 results = collection.aggregate(parsed_args)
#             else:
#                 return jsonify({"error": "Invalid arguments for 'aggregate'."}), 400
#         else:
#             return jsonify({"error": f"Unsupported method '{method}'."}), 400

#         # Step 7: Serialize and return the results as JSON
#         data = json.loads(dumps(results))
#         response_data = {
#             "query": mongodb_query,  # Include the query as part of the data
#             "results": data          # Include the original query results
#         }
#         print(f"Final MongoDB Response: {response_data}")
#         return jsonify({"data": response_data}), 200

#     except Exception as e:
#         print(f"Error: {e}")
#         return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/nlq-to-mongo', methods=['POST'])
def nlq_to_mongo():
    try:
        # Step 1: Get the NLQ from the request
        nlq = request.json.get('nlq')
        if not nlq:
            return jsonify({"error": "No NLQ provided."}), 400

        print(f"Received NLQ: {nlq}")

        # Step 2: Convert NLQ to SQL
        global curr_table
        pattern_key, placeholders = queryMapper.parse_input(nlq, curr_table)
        sql_query = queryMapper.generate_query(pattern_key, placeholders)
        if not sql_query:
            return jsonify({"error": "Failed to generate SQL query."}), 500

        # Debug SQL query before and after removing the semicolon
        print(f"Generated SQL Query (raw): {sql_query}")
        sql_query = sql_query.rstrip(';')
        print(f"Generated SQL Query (cleaned): {sql_query}")

        # Step 3: Convert SQL query to MongoDB query
        mongodb_query = sql_to_mongodb(sql_query)
        if not mongodb_query:
            return jsonify({"error": "Failed to generate MongoDB query."}), 500

        print(f"Generated MongoDB Query: {mongodb_query}")
        print(f"Returning MongoDB Query: {mongodb_query}")

        # Step 4: Parse the MongoDB query
        match = re.match(r"db\.(\w+)\.(\w+)\((.*)\)", mongodb_query.strip())
        if not match:
            return jsonify({"error": "Invalid MongoDB query format after conversion"}), 400

        collection_name, method, args = match.groups()
        print(f"Parsed MongoDB - Collection: {collection_name}, Method: {method}, Args: {args}")

        # Step 5: Access the MongoDB collection
        collection = mongo_db[collection_name]
        print("Collection Name:", collection)

        # Safely evaluate the query arguments
        # **Modification: Replace literal_eval with json.loads**
        try:
            # Ensure that 'args' is a valid JSON string
            parsed_args = json.loads(args) if args else {}
            print(f"Parsed Arguments for MongoDB Query: {parsed_args}")
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Invalid query arguments: {str(e)}"}), 400

        # Step 6: Execute the MongoDB query
        if method == "find":
            # Handle `find` queries
            if isinstance(parsed_args, dict):
                results = collection.find(parsed_args)
            elif isinstance(parsed_args, list) and len(parsed_args) == 2:
                results = collection.find(*parsed_args)  # Filter and projection
            else:
                return jsonify({"error": "Invalid arguments for 'find'."}), 400
        elif method == "aggregate":
            # Handle `aggregate` queries
            if isinstance(parsed_args, list):
                results = collection.aggregate(parsed_args)
            else:
                return jsonify({"error": "Invalid arguments for 'aggregate'."}), 400
        else:
            return jsonify({"error": f"Unsupported method '{method}'."}), 400

        # Step 7: Serialize and return the results as JSON
        data = json.loads(dumps(results))
        response_data = {
            "query": mongodb_query,  # Include the query as part of the data
            "results": data          # Include the original query results
        }
        print(f"Final MongoDB Response: {response_data}")
        return jsonify({"data": response_data}), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# @app.route('/query/mongodb', methods=['POST'])
# def execute_full_mongodb_query():
#     try:
#         # Extract the raw query from request parameters
#         #raw_query = request.json.get('query') 
#         raw_query = request.json.get('query') if request.method == 'POST' else request.args.get('query')
#         print(f"Received raw query in backend: {raw_query}")
#         #raw_query = request.args.get('query')
#         if not raw_query:
#             return jsonify({"error": "Query parameter must be provided."}), 400

#         # Debug log to verify raw query
#         print(f"Received raw query: {raw_query}")

#         # Extract collection name, method, and arguments from the query
#         import re
#         match = re.match(r"db\.(\w+)\.(\w+)\((.*)\)", raw_query.strip())
#         if not match:
#             return jsonify({"error": "Invalid query format. Expected 'db.collection.method(...)'."}), 400

#         # Parse the collection name, method, and arguments
#         collection_name, method, args = match.groups()
#         print(f"Parsed collection: {collection_name}, method: {method}, args: {args}")

#         # Access the MongoDB collection
#         collection = mongo_db[collection_name]

#         # Parse the query arguments safely
#         from ast import literal_eval
#         try:
#             parsed_args = literal_eval(args) if args else {}
#         except Exception as e:
#             return jsonify({"error": f"Invalid query arguments: {str(e)}"}), 400

#         # Determine and execute the method dynamically
#         if method == "find":
#             # Execute the `find` query
#             if isinstance(parsed_args, dict):
#                 results = collection.find(parsed_args)
#             elif isinstance(parsed_args, list) and len(parsed_args) == 2:
#                 results = collection.find(*parsed_args)  # Handle filter and projection
#             else:
#                 return jsonify({"error": "Invalid arguments for 'find'."}), 400
#         elif method == "aggregate":
#             # Execute the `aggregate` query
#             if isinstance(parsed_args, list):
#                 results = collection.aggregate(parsed_args)
#             else:
#                 return jsonify({"error": "Invalid arguments for 'aggregate'."}), 400
#         else:
#             return jsonify({"error": f"Unsupported method '{method}'."}), 400

#         # Serialize the results to JSON and return
#         return jsonify({"data": json.loads(dumps(results))})
#     except Exception as e:
#         return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/query/mongodb', methods=['POST', 'GET'])
def execute_full_mongodb_query():
    try:
        # Extract the raw query from request parameters
        if request.method == 'POST':
            raw_query = request.json.get('query')
        else:
            raw_query = request.args.get('query')
        
        print(f"Received raw query in backend: {raw_query}")
        
        if not raw_query:
            return jsonify({"error": "Query parameter must be provided."}), 400

        # Debug log to verify raw query
        print(f"Received raw query: {raw_query}")

        # Extract collection name, method, and arguments from the query
        match = re.match(r"db\.(\w+)\.(\w+)\((.*)\)", raw_query.strip())
        if not match:
            return jsonify({"error": "Invalid query format. Expected 'db.collection.method(...)'."}), 400

        # Parse the collection name, method, and arguments
        collection_name, method, args = match.groups()
        print(f"Parsed collection: {collection_name}, method: {method}, args: {args}")

        # Access the MongoDB collection
        if collection_name not in mongo_db.list_collection_names():
            return jsonify({"error": f"Collection '{collection_name}' does not exist."}), 400
        collection = mongo_db[collection_name]
        print(f"Accessed collection: {collection_name}")

        # **Modification: Replace literal_eval with json.loads**
        try:
            # Ensure that 'args' is a valid JSON string
            # Handle cases where args might not be a valid JSON array or object
            if method == "find":
                # 'find' can have either a filter (object) or [filter, projection] (array)
                if args.startswith('[') and args.endswith(']'):
                    parsed_args = json.loads(args)
                else:
                    parsed_args = json.loads(args) if args else {}
            elif method == "aggregate":
                # 'aggregate' expects an array
                parsed_args = json.loads(args) if args else []
            else:
                # For other methods, adjust as needed
                parsed_args = json.loads(args) if args else {}
            print(f"Parsed Arguments for MongoDB Query: {parsed_args}")
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Invalid query arguments: {str(e)}"}), 400

        # Determine and execute the method dynamically
        if method == "find":
            # Execute the `find` query
            if isinstance(parsed_args, dict):
                results = collection.find(parsed_args)
            elif isinstance(parsed_args, list) and len(parsed_args) == 2:
                results = collection.find(*parsed_args)  # Handle filter and projection
            else:
                return jsonify({"error": "Invalid arguments for 'find'."}), 400
        elif method == "aggregate":
            # Execute the `aggregate` query
            if isinstance(parsed_args, list):
                results = collection.aggregate(parsed_args)
            else:
                return jsonify({"error": "Invalid arguments for 'aggregate'."}), 400
        else:
            return jsonify({"error": f"Unsupported method '{method}'."}), 400

        # Serialize the results to JSON and return
        serialized_results = json.loads(dumps(results))
        return jsonify({"data": serialized_results}), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# Upload Dataset to MongoDB
import json

@app.route('/upload/mongodb', methods=['POST'])
def upload_json_to_mongodb():
    try:
        file = request.files['file']
        collection_name = request.form['collection']
        global curr_table
        curr_table = collection_name
        # Load JSON file content
        file_data = json.load(file)  # Read and parse the JSON content
        if isinstance(file_data, list):  # Ensure it's a list of documents
            documents = file_data
        else:
            return jsonify({"error": "The uploaded JSON must be an array of objects."}), 400

        # Insert documents into MongoDB
        collection = mongo_db[collection_name]
        collection.insert_many(documents)

        return jsonify({"message": f"JSON data {curr_table} uploaded successfully to MongoDB."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# Execute Query in MongoDB
# from bson.json_util import dumps
# @app.route('/query/mongodb', methods=['POST'])
# def execute_mongodb_query():
#     try:
#         # Extract MongoDB query string from the request
#         raw_query = request.json.get('query')
#         collection_name = request.json.get('collection')
        
#         if not raw_query or not collection_name:
#             return jsonify({"error": "Collection and query must be provided."}), 400

#         # Safely evaluate the raw_query into a Python dictionary
#         try:
#             # Use `literal_eval` to avoid running arbitrary code
#             from ast import literal_eval
#             query = literal_eval(raw_query)
#         except Exception as e:
#             return jsonify({"error": f"Invalid MongoDB query format: {str(e)}"}), 400

#         # Access the specified MongoDB collection
#         collection = mongo_db[collection_name]

#         # Execute the query
#         results = collection.find(query)

#         # Serialize results with bson.json_util.dumps
#         return jsonify({"data": json.loads(dumps(results))})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @app.route('/query/mongodb', methods=['POST'])
# def execute_mongodb_query():
#     try:
#         query = request.json['query']
#         collection_name = query['collection']
#         filter_query = query.get('filter', {})

#         # Access the MongoDB collection
#         collection = mongo_db[collection_name]
#         results = collection.find(filter_query)

#         # Serialize results with bson.json_util.dumps
#         return jsonify({"data": json.loads(dumps(results))})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500



# Fetch MongoDB Metadata
@app.route('/metadata/mongodb', methods=['GET'])
def get_mongodb_metadata():
    try:
        collections = mongo_db.list_collection_names()
        metadata = {}
        for collection in collections:
            sample_doc = mongo_db[collection].find_one()
            metadata[collection] = list(sample_doc.keys()) if sample_doc else []
        return jsonify(metadata)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    



#Delete collection from MongoDB
@app.route('/delete/mongodb', methods=['POST'])
def delete_mongodb_collection():
    try:
        collection_name = request.json['collection']

        # Check if the collection exists
        if collection_name in mongo_db.list_collection_names():
            mongo_db[collection_name].drop()
            return jsonify({"message": f"Collection `{collection_name}` deleted successfully."})
        else:
            return jsonify({"message": f"Collection `{collection_name}` does not exist."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# Run the Flask App
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True)
