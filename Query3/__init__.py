import logging
import os

import pyodbc

import azure.functions as func

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]

BATCH_SIZE = 10000

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    try:
        result = get_avg_ratings()
        resultString = "Liste des notes moyennes des films par genre :\n\n"
        for genre, rating in result.items():
            resultString += f"{genre} : {round(rating,2)}/10\n"
        return func.HttpResponse("Connexion réussie à SQL Server!\n\n" + resultString)

    except:
        return func.HttpResponse("Erreur de connexion à la base SQL Server", status_code=500)

def get_avg_ratings():
    resultData = {}
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        
        query = "SELECT genre, SUM(averageRating)/COUNT(*) AS avg_rating\
            FROM [dbo].[tTitles] AS t\
            INNER JOIN [dbo].[tGenres] g ON t.tconst = g.tconst \
            WHERE averageRating >= 0\
            GROUP BY genre"

        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            
            for row in rows:                
                resultData[row[0]]=row[1]

    return resultData
