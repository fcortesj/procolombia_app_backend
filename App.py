
from flask import Flask, json, render_template, request,jsonify
from bs4 import BeautifulSoup
import urllib.request
import sys
import time
import requests
import lxml
import pandas as pd
import random
import boto3
import os
import pandas as pd
import io
import numpy as np
import random

app = Flask(__name__) 

from extract_main_info import database
from get_info_by_company import info_company

@app.route("/") #login of any user
def Index():
    return render_template("Index.html") # run the frondend



@app.route("/quest",methods=["POST"]) #post type action
def Quest(): #function that executes the filted from the database
    if request.method=="POST":
        id = [request.form["id1"]] # read identifier!
        stocker=[]
        for iterator in id:
            name = info_company().return_data_from_company(iterator)
            stocker.append(name)
        file_json=json.dumps(stocker, ensure_ascii=False)
    return (file_json)

@app.route("/news_scrapper",methods=["POST"]) 
def News_Scrapper(): #function news
    if request.method=="POST":
        id = [request.form["id"]] # read identifier!
        #id = "Procolombia" 
        def news_scrapper(enterprises_list): #function news_scrapper
            result_df = pd.DataFrame(columns=['enterprise', 'title', 'link', 'source', 'snippet', 'date_published'])
            for enterprise in enterprises_list:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
                }
                params = {
                    "q": str(enterprise).lower(),
                    "tbm": "nws"
                }
                response = requests.get(
                    "https://www.google.com/search", headers=headers, params=params)
                soup = BeautifulSoup(response.text, 'lxml')
                result_l = soup.select('.dbsr')
                if len(result_l) >= 3:
                    random_news = random.sample(result_l, 3)
                else:
                    random_news = result_l
                for news in random_news:
                    title = news.select_one('.nDgy9d').text
                    link = news.a['href']
                    source = news.select_one('.WF4CUc').text
                    snippet = news.select_one('.Y3v8qd').text
                    date_published = news.select_one('.WG9SHc span').text
                    result_df.loc[-1] = [str(enterprise), str(title), str(link),
                                        str(source), str(snippet), str(date_published)]
                    result_df.index = result_df.index + 1
                    result_df = result_df.sort_index()

            return (result_df)

        file = json.loads(news_scrapper(id).to_json(orient = 'records'))
        file_json=json.dumps(file, ensure_ascii=False)
        return (file_json)

@app.route("/database",methods=["POST"]) 
def Database(): #function database
    id = request.form["id2"] # read identifier in string
    if request.method=="POST":
        name = database(id).info()
        dataframe = pd.DataFrame([[key, name[key]] for key in name.keys()])
        file = json.loads(dataframe.to_json(orient = 'records'))
        file_json=json.dumps(file, ensure_ascii=False)    
        return (file_json)

@app.route("/database_cantidad_empresas_por_sector",methods=["POST"]) 
def Database_cantidad_empresas_por_sector(): #function database
    id = request.form["id4"] # read identifier 
    if request.method=="POST":
        name = database(id).info()
        dataframe_cantidad_empresas_por_sector=pd.DataFrame(list(map(name.get, ["cantidad_empresas_por_sector"])))
        file = json.loads(dataframe_cantidad_empresas_por_sector.to_json(orient = 'records'))
        file_json=json.dumps(file, ensure_ascii=False)    
        return (file_json)

@app.route("/get_list_companies",methods=["POST"]) 
def Get_list_companies(): #function get_list
    id = request.form["id4"] # read identifier 
    if request.method=="POST":
        def get_top_scores(df, n_scores=11, avoid_list=[], col_score="score", name="clean_name"):
            """
            Input: DataFrame, it must contain a score column
            Output: DataFrame with n_scores rows, priorized by top scores
            Arguments:
                - n_scores: number of rows to get
                - avoid_list: names of companies to avoid
                - col_score: name of the score column
                - name: name of the bussiness name column
            """
            df_sorted = df.sort_values(by=col_score, ascending=False)
            df_top = pd.DataFrame()
            cnt = 0
            while n_scores > 1:
                top_to_add = df_sorted.iloc[cnt]
                cnt += 1
                if top_to_add[name] in avoid_list:
                    continue
                else:
                    df_top = df_top.append(top_to_add)
                    avoid_list.append(top_to_add[name])
                    n_scores -= 1

            df_top = df_top.reset_index(drop=True)
            return df_top
        def get_list_companies(df, cols_to_keep=None):
            """ Returns the dataframe as a list of dictionaries  """
            if cols_to_keep is None:
                cols_to_keep = ['name', 'score', 'source_country']

            list_companies = df.filter(cols_to_keep).to_dict('records')
            return list_companies
        def run_get_list_companies(n_companies=11):
            """
            Runs with default values the following:
            1. Downloads the joined final_database
            2. Finds the most promising n_companies (defaults to 10)
            3. Returns a list with dictionaries
                - Each row is a datapoint in the array
                - The diccinary matches the columns keep by the function get_list_companies
                    - Default: 'name', 'score', 'source_country'
            """
            # Set up the resources
            s3 = boto3.resource(
                    service_name='s3',
                    region_name='us-east-1',
                    aws_access_key_id='your aws access key',
                    aws_secret_access_key='your aws secret key'
                )

            s3c = boto3.client(
                    's3', 
                    region_name='us-east-1',
                    aws_access_key_id='your aws access key',
                    aws_secret_access_key='your aws secret key'
                )

            # Important variables
            bucket_name   = 'ds4a5-team-50'
            temp_object   = s3c.get_object(Bucket=bucket_name, Key='data/preprocessed/00_final_database.csv')
            df_total      = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep=',', index_col=0).drop_duplicates()

            # getting the top companies by score as dataframe
            list_2exclude = ['almacenes exito']
            promising_companies = get_top_scores(df_total, n_companies, list_2exclude, name='name')

            # transforming dataframe into list of dictionaries
            return get_list_companies(promising_companies)
        return jsonify(run_get_list_companies(int(id)+1)) # use the funcion run_get_list_companies the argument is a int, 1 is added to the id data so that there is a congruence  


if __name__ == "__main__": #start the server
    app.run(port=3000, debug=True)

