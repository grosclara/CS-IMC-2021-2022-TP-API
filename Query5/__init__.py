import logging
import os

import pyodbc

import azure.functions as func

BATCH_SIZE=100000

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    category = get_param(req, "category")
    director = get_param(req, 'director')
    actor = get_param(req, 'actor')

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    try:
        result = get_avg_duration(category, director, actor)
        resultString = f"Durée moyenne des films filtés selon les critères suivants [genre: {category}, acteur: {actor}, directeur: {director}] : {result} minutes."
        return func.HttpResponse("Connexion réussie à SQL Server!\n\n" + resultString)

    except:
        return func.HttpResponse("Erreur de connexion à la base SQL Server", status_code=500)


def get_avg_duration(category, director, actor):

    result = -1

    cat_query = f"SELECT t.tconst, runtimeMinutes\
        FROM [dbo].[tTitles] as t\
        INNER JOIN [dbo].[tGenres] g\
        WHERE t.tconst = g.tconst \
        AND genre = '{category}'" if category else None

    act_query = f"SELECT t.tconst, runtimeMinutes\
        FROM [dbo].[tTitles] AS t\
        INNER JOIN [dbo].[tPrincipals] a\
        ON T.tconst = a.tconst\
        INNER JOIN [dbo].[tNames] b\
        ON b.nconst = a.nconst\
        WHERE category = 'acted in' AND primaryName = {actor}" if actor else None

    dir_query = f"SELECT t.tconst, runtimeMinutes\
        FROM [dbo].[tTitles] AS t\
        INNER JOIN [dbo].[tPrincipals] a\
        ON T.tconst = a.tconst\
        INNER JOIN [dbo].[tNames] b\
        ON b.nconst = a.nconst\
        WHERE category = 'directed' AND primaryName = '{director}'" if director else None

    if not cat_query and not dir_query and not act_query:
        query = "SELECT SUM(runtimeMinutes)/COUNT(*) AS avg_duration FROM [dbo].[tTitles]"
    else:
        filters = ' INTERSECT '.join(filter(None, [cat_query, act_query, dir_query]))
        query = f"WITH req1 AS ( {filters} ) SELECT SUM(runtimeMinutes)/COUNT(*) AS avg_minutes FROM req1"

    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()

        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if rows:
                result = rows[0][0]
            break

    return result


def get_param(req, param):

    value = req.params.get(param)
    if not value:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            value = req_body.get(param)

    return value
