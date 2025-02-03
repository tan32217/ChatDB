The ChatDB project bridges the gap between natural language and database querying by enabling users to interact with databases using natural language. This eliminates the need for technical expertise in SQL or NoSQL query writing, empowering users to extract valuable insights easily.

The platform uses NLP techniques to process user inputs and dynamically generate database-specific queries, ensuring flexibility and usability.

Flow Diagram
<img width="644" alt="image" src="https://github.com/user-attachments/assets/d9dd484b-25e8-4b81-95fc-59b26c010d52" />

Description
1. Frontend Interaction
User Input: Users enter natural language queries or raw SQL/NoSQL queries via the Angular-based UI.
There’s also a drop down that allows the selection of the database type (MySQL or MongoDB) for upload.
Queries are generated along with the results and are  displayed in a table format or as pretty-printed JSON depending on the database type.

2. Backend Processing
The backend processes these inputs using Flask and dedicated endpoints:
Query Handling: Natural Language Queries are passed to, for generating SQL/NoSQL queries. Preprocessed user input to extract keywords, mapping them to corresponding column names, table names, and database operations for accurate query generation.
NLQ_to_SQL: Uses processed input and converts them into SQL queries suitable for MySQL.
NLQ_to_NoSQL: Uses processed input in NoSQL aggregation pipelines for MongoDB.
Query Execution:
MySQL: SQL queries are executed using mysql-connector-python.
MongoDB: NoSQL queries are translated to aggregation pipelines and executed via PyMongo.

4. Database Integration
Backend modules connect to MySQL and MongoDB databases to execute the translated queries.

5. Results Display
Flask sends processed results back to the frontend:
SQL Results: Rendered as tables in the UI.
NoSQL Results: Displayed as aggregated data or pretty-printed JSON for better readability.

Screen Shots:

Uploading Dataset
<img width="632" alt="image" src="https://github.com/user-attachments/assets/b89de7af-b3b1-4ce7-9f48-5e1334788fd5" />

Display Data
<img width="633" alt="image" src="https://github.com/user-attachments/assets/0eab104b-3910-4ec3-987c-686c4704717a" />

Query Execution
<img width="611" alt="image" src="https://github.com/user-attachments/assets/7dc3d60e-0b85-4c03-bf15-a94eb57399c1" />





