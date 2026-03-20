import os

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))


class GraphManager:
    def __init__(self):
        self.driver = None
        if not URI or not AUTH[0] or not AUTH[1]:
            raise RuntimeError("Missing Neo4j credentials: set NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD.")
        try:
            self.driver = GraphDatabase.driver(URI, auth=AUTH)
            self.driver.verify_connectivity()
            print("Connected to Neo4j")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Neo4j: {e}") from e

    def close(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None


def get_verified_driver():
    return GraphManager().driver