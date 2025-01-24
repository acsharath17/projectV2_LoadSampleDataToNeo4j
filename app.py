from neo4j import GraphDatabase
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Load environment variables
load_dotenv()

uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")

def upload_data(uri, username, password, data):
    """
    Uploads data to a Neo4j AuraDB instance.

    Parameters:
        uri (str): The URI of the Neo4j AuraDB instance.
        username (str): The username for authentication.
        password (str): The password for authentication.
        data (list of dict): The data to be uploaded, structured as a list of dictionaries.
    """
    try:
        # Create a Neo4j driver instance
        driver = GraphDatabase.driver(uri, auth=(username, password))

        # Define a function to create nodes and relationships in a session
        def upload(tx, data):
            for item in data:
                query = """
                MERGE (n:Person {name: $name})
                """
                tx.run(query, name=item["name"])

        # Open a session and upload data
        with driver.session() as session:
            session.execute_write(upload, data)

        return {"status": "success", "message": "Data uploaded successfully!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        driver.close()

@app.route('/upload', methods=['POST'])
def upload():
    if not username or not password:
        return jsonify({"status": "error", "message": "Please set NEO4J_USERNAME and NEO4J_PASSWORD in your .env file."}), 500

    try:
        data = request.json
        if not data or not isinstance(data, list):
            return jsonify({"status": "error", "message": "Invalid input. Expected a list of dictionaries."}), 400

        response = upload_data(uri, username, password, data)
        return jsonify(response)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
