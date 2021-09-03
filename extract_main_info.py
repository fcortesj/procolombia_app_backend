from flask import Flask, json, render_template, request,jsonify
import boto3
import os
import pandas as pd
import io
import numpy as np
import random
class database():
    def __init__(self,name):
        self.name=name
    def info(self):
        pd.options.mode.chained_assignment = None
        # Important variables
        bucket_name = 'ds4a5-team-50'
        folder_prefix = 'data/raw/'

        # Set up the resources

        s3 = boto3.resource(
                service_name='s3',
                region_name='us-east-1',
                aws_access_key_id='your aws acces key',
                aws_secret_access_key='your aws secret key'
            )

        s3c = boto3.client(
                's3', 
                region_name='us-east-1',
                aws_access_key_id='your aws acces key',
                aws_secret_access_key='your aws secret key'
            )

        # Lets get the bucket
        bucket = s3.Bucket(bucket_name) 
        response = bucket.objects.filter(Prefix=folder_prefix)

        # Perprocess Functions
        def get_clean_names(sr_name, rem_punkt=0):                        
            sr_name = sr_name.str.lower()                                 
            if rem_punkt: sr_name = sr_name.str.replace(r'[^\w\s]', '')   
            sr_name = sr_name.str.strip().str.replace(r'\s{2}', '')      
            return sr_name

        def clean_price(row):
            try:
                clean_row = row.replace('$','').replace('.','').strip()
                f = float(clean_row)
                return float(clean_row)
            except:
                return 0

        def clean_description(row):
            try:
                cleaned_des = row.split(' - ')[1].strip()
                return cleaned_des
            except:
                return 'Engineering Services'

        def clean_employees(row):
            try:
                num = int(row)
            except:
                return 0
        # Function
        database_key = 'data/raw/' + str(self.name)
        identifier = database_key.split('/')[-1].split('_')[0]
        temp_object = s3c.get_object(Bucket=bucket_name, Key=database_key)
        data_to_return = {}
        if identifier == '00':
            df = pd.read_excel(io.BytesIO(temp_object['Body'].read()), parse_dates=['Fecha de inversión'], dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['Nombre'])
            total_enterprises = df['Name'].count()
            total_sectors = df['Sector'].count()
            total_market_chain = df['Cadena productiva'].count()
            max_countries = len(df.groupby('Mercado'))
            count_by_sector = df.groupby('Sector').agg('count')['Cadena productiva'].sort_values(ascending=False)
            count_by_country = df.groupby('Mercado').agg('count')['Cadena productiva'].sort_values(ascending=False)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['total_sectores'] = total_sectors
            data_to_return['total_cadena_productiva'] = total_market_chain
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['cantidad_empresas_por_pais'] = count_by_country
        if identifier == '01':
            df = pd.read_excel(io.BytesIO(temp_object['Body'].read()), sheet_name='Base instalados', skiprows=9, dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['Nombre registrado en RUE'])
            total_enterprises = df['Name'].count()
            df['Empleados'] = df.apply(lambda x: clean_employees(x['Empleados']), axis = 1)
            mean_employees = df['Empleados'].mean()
            df['Ingresos operacionales COP miles 2013'] = df.apply(lambda x : clean_price(x['Ingresos operacionales COP miles 2013']), axis = 1)
            mean_investment = df['Ingresos operacionales COP miles 2013'].mean()
            max_countries = 1
            count_by_sector = df.groupby('Sector').agg('count')['Gerencia'].sort_values(ascending=False)
            grouped = df[['Sector', 'Ingresos operacionales COP miles 2013']].sort_values(by=['Ingresos operacionales COP miles 2013'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['Sector']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['inversion_promedio'] = mean_investment
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['total_paises_por_sector'] = grouped
        if identifier == '02':
            df = pd.read_excel(io.BytesIO(temp_object['Body'].read()), sheet_name='SocExt', skiprows=11, dtype=str, na_values=0).drop_duplicates()
            df = df.dropna()
            df['Name'] = get_clean_names(df['Razón Social'])
            total_enterprises = df['Name'].count()
            df['Empleados'] = pd.to_numeric(df['Empleados'], downcast="integer")
            mean_employees = df['Empleados'].mean()
            df['Ingresos operacionales\nMiles de pesos'] = df.apply(lambda x : clean_price(x['Ingresos operacionales\nMiles de pesos']), axis = 1)
            mean_investment = df['Ingresos operacionales\nMiles de pesos'].mean()
            max_countries = 1
            count_by_sector = df.groupby('Descripción actividad económica').agg('count')['Departamento Comercial'].sort_values(ascending=False)
            grouped = df[['Descripción actividad económica', 'Ingresos operacionales\nMiles de pesos']].sort_values(by=['Ingresos operacionales\nMiles de pesos'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['Descripción actividad económica']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['inversion_promedio'] = mean_investment
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['total_paises_por_sector'] = grouped
        if identifier == '03':
            df = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep=',', encoding='latin-1', dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['InvestingCompany'])
            total_enterprises = df['Name'].count()
            df['JobsCreated'] = pd.to_numeric(df['JobsCreated'], downcast="integer")
            mean_employees = df['JobsCreated'].mean()
            df['CapitalInvestment'] = df.apply(lambda x : clean_price(x['CapitalInvestment']), axis = 1)
            mean_investment = df['CapitalInvestment'].mean()
            max_countries = len(df.groupby('SourceCountry'))
            count_by_sector = df.groupby('IndustrySector').agg('count')['Cluster'].sort_values(ascending=False)
            grouped = df[['IndustrySector', 'CapitalInvestment']].sort_values(by=['CapitalInvestment'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['IndustrySector']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['inversion_promedio'] = mean_investment
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['total_paises_por_sector'] = grouped 
        if identifier == '04':
            df = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep=';', dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['Name'])
            df = df.dropna()
            total_enterprises = df['Name'].count()
            df[' Revenue '] = df.apply(lambda x : clean_price(x[' Revenue ']), axis = 1)
            df[' Revenue '] = pd.to_numeric(df[' Revenue '], downcast="float")
            mean_revenue = df[' Revenue '].mean()
            df['Employees'] = pd.to_numeric(df['Employees'], downcast="integer")
            mean_employees = df['Employees'].mean()
            df['Presencia Número de países'] = pd.to_numeric(df['Presencia Número de países'], downcast="integer")
            max_countries = df['Presencia Número de países'].max()
            df['sector'] = df.apply(lambda x : clean_description(x['Primary Naics']), axis = 1)
            count_by_sector = df.groupby('sector').agg('count')['ID'].sort_values(ascending=False)
            grouped = df.groupby(['sector','Presencia Número de países']).agg('count')['ID'].sort_values(ascending=False).reset_index()
            grouped = grouped.drop(['ID'], axis=1)
            grouped = grouped.sort_values(by='Presencia Número de países', ascending=False).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['ganancia_promedio'] = mean_revenue
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['total_paises_por_sector'] = grouped 
        if identifier == '05':
            df = pd.read_excel(io.BytesIO(temp_object['Body'].read()), sheet_name='Sheet1', skiprows= 2, dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['Investing company'])
            total_enterprises = df['Name'].count()
            df['Jobs created'] = df.apply(lambda x: clean_employees(x['Jobs created']), axis = 1)
            df['Jobs created'] = pd.to_numeric(df['Jobs created'], downcast="integer")
            mean_employees = df['Jobs created'].mean()
            df['Capital investment'] = df.apply(lambda x : clean_price(x['Capital investment']), axis = 1)
            mean_investment = df['Capital investment'].mean()
            max_countries = len(df.groupby('Source country'))
            count_by_sector = df.groupby('Sector').agg('count')['Cluster'].sort_values(ascending=False)
            grouped = df[['Sector', 'Capital investment']].sort_values(by=['Capital investment'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['Sector']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['inversion_promedio'] = mean_investment
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['total_paises_por_sector'] = grouped 
        if identifier == '06':
            df = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep=';', dtype=str).drop_duplicates()
            df = df.dropna()
            df['Name'] = get_clean_names(df['Name'])
            total_enterprises = df['Name'].count()
            df['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'] = df.apply(lambda x : clean_price(x['Ingresos de explotación (turnover)\nmil USD Últ. año disp.']), axis = 1)
            mean_revenue = df['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'].mean()
            df['Número de empleados\nÚlt. año disp.'] = df.apply(lambda x : clean_employees(x['Número de empleados\nÚlt. año disp.']), axis = 1)
            mean_employees = df['Número de empleados\nÚlt. año disp.'].mean()
            max_countries = len(df.groupby('País'))
            count_by_sector = df.groupby('Actividad principal').agg('count')['ID'].sort_values(ascending=False)
            grouped = df[['Actividad principal', 'Ingresos de explotación (turnover)\nmil USD Últ. año disp.']].sort_values(by=['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['Actividad principal']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['ingresos_promedio'] = mean_revenue
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['max_ingresos_por_sector'] = grouped 
        if identifier == '07':
            df = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep="~", index_col=0, dtype=str, na_values='n.d.')
            df = df.dropna()
            df['Name'] = get_clean_names(df['Nombre empresaAlfabeto latino'])
            total_enterprises = df['Name'].count()
            df['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'] = df.apply(lambda x : clean_price(x['Ingresos de explotación (turnover)\nmil USD Últ. año disp.']), axis = 1)
            mean_revenue = df['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'].mean()
            df['Número de empleados\nÚlt. año disp.'] = df.apply(lambda x : clean_employees(x['Número de empleados\nÚlt. año disp.']), axis = 1)
            mean_employees = df['Número de empleados\nÚlt. año disp.'].mean()
            max_countries = len(df.groupby('País'))
            count_by_sector = df.groupby('Actividad principal').agg('count')['Name'].sort_values(ascending=False)
            grouped = df[['Actividad principal', 'Ingresos de explotación (turnover)\nmil USD Últ. año disp.']].sort_values(by=['Ingresos de explotación (turnover)\nmil USD Últ. año disp.'],ascending=False)
            grouped = grouped.drop_duplicates(subset=['Actividad principal']).reset_index(drop=True)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['ingresos_promedio'] = mean_revenue
            data_to_return['empleados_promedio'] = mean_employees
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['max_ingresos_por_sector'] = grouped 
        if identifier == '08':
            df = pd.read_csv(io.BytesIO(temp_object['Body'].read()), sep=';', dtype=str).drop_duplicates()
            df['Name'] = get_clean_names(df['Name'])
            total_enterprises = df['Name'].count()
            total_sectors = df['Objeto social principal'].count()
            total_market_chain = df['Domicilio casa matriz sucursal de sociedad extranjera'].count()
            max_countries = 1
            count_by_sector = df.groupby('Objeto social principal').agg('count')['Estado actual'].sort_values(ascending=False)
            count_by_country = df.groupby('Domicilio casa matriz sucursal de sociedad extranjera').agg('count')['Estado actual'].sort_values(ascending=False)
            data_to_return['total_empresas'] = total_enterprises
            data_to_return['total_sectores'] = total_sectors
            data_to_return['total_casas_extranjeras'] = total_market_chain
            data_to_return['max_paises_abarcados'] = max_countries
            data_to_return['cantidad_empresas_por_sector'] = count_by_sector 
            data_to_return['cantidad_por_casa_extranjera'] = count_by_country 

        return  data_to_return
    

