from flask import Blueprint, request, jsonify
from app.services import process_data

ingestion_blueprint = Blueprint('ingestion', __name__)

@ingestion_blueprint.route('/ingest', methods=['POST'])
def ingest():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Process the data (validate and forward to Kafka)
    response = process_data(data)
    if "error" in response:
        return jsonify(response), 400

    return jsonify(response), 200
