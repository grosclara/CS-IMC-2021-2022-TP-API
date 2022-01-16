import logging
import os

from py2neo import Graph

import azure.functions as func

neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
neo4j_user = os.environ["TPBDD_NEO4J_USER"]
neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    try:
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        result = graph.run("MATCH (n:Name) WHERE n.birthYear=1960 RETURN COUNT (*)").data()[0]['COUNT (*)']
        resultString = f"Nombre d'artistes nés en 1960: {result}"
        return func.HttpResponse("Connexion réussie à Neo4j!\n\n" + resultString)

    except:
        return func.HttpResponse("Erreur de connexion à la base Neo4j", status_code=500)
