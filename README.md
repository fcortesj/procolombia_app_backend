# procolombia_app_backend
 Backend por procolombias application. All services are exposed through an API Rest server. 
 The main purpose for this app is returing model and data insights to the front app in order
 to display them in a front-back oriented web application.

## Running App Address:
http://ec2-54-221-171-55.compute-1.amazonaws.com:8000/

## Required libraries:
 flask, beautifulsoup4, requests, lxml, pandas, boto3 
 
## Command to run in the terminal: 
 py App.py

### Methods and description

##### main start path (/)
    The main resource of the app

##### search a company (/quest)
	input: list or string
	example: ["teleperformance south cone", "fleischmann"]
    Method which returns the description of a given company. Also, the elements to plot a graph between the company and the top companies of the database.
    

##### news scrapper (/news_scrapper)
	input: list or string
	example: ["Tesla", "Procolombia"]
    Web Scrapper to search news of a given list of companies. The lis contains the news title, link, date, etc.

##### info for main frond and the graphic (/database)
	input: string 
	example: "06_i_orbis.csv"
    This resource shows the main info that is displayed on the dashboard including elements of the dataset and main graphs.

##### info for main frond and the graphic only data "cantidad_empresas_por sector"
	input: string 
	example: "06_i_orbis.csv"
    This method return the graph which displays the total companies by sector.

##### promising companies (/get_list_companies)
	input: string (but number)
	example "5"
    Resource used to get the top model scores including all companies in the unified database.



