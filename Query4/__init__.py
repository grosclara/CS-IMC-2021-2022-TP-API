import logging
import os

from py2neo import Graph

import azure.functions as func

server = os.environ["TPBDD_NEO4J_SERVER"]
username = os.environ["TPBDD_NEO4J_USER"]
password = os.environ["TPBDD_NEO4J_PASSWORD"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if len(server)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            
    if name:
        try:
            birthYear = get_birthYear(name)
            if birthYear < 0:
                return func.HttpResponse("Connexion réussie à SQL Server!\n\n" + f"There is no artist named {name} in the database.")
            return func.HttpResponse("Connexion réussie à SQL Server!\n\n" + f"{name} was born in {birthYear}.")
        except:
            return func.HttpResponse("Erreur de connexion à la base Neo4J", status_code=500)

    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def get_birthYear(name):
    result = -1

    graph = Graph(server, auth=(username, password))
    response = graph.run(f'MATCH (n:Name) WHERE n.primaryName="{name}" RETURN n.birthYear').data()

    if response:
        result = response[0]['n.birthYear']
        
    return result
