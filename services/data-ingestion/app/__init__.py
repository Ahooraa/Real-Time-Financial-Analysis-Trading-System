from flask import Flask
from app.routes import ingestion_blueprint

def create_app():
    app = Flask(__name__)

    # Register routes
    app.register_blueprint(ingestion_blueprint)

    return app
