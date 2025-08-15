## Automated Stock Data Pipelinu Using Dagster
This project provides an Dockerized stock data pipeline automated using Dagster. The pipeline fetches stock market data for each stock, every 5 minutes and stores it in a Postgres database. 
The data in DB is displayed using a Flask Endpoint.

##Setting Up
1. Install Docker Desktop and Run it
2. Clone the Repository
3. Open the command prompt for this directory and pull the image from docker using
``` bash
$ docker pull skandasshetty846/8byteai-stock-pipeline:latest
```
4. Build the project
``` bash
$ docker compose up -d
```
5. Pipeline is scheduled to fetch data every 5 minutes.

You can view the Dagster logs using command
``` bash
$ docker logs 'Your-dagster-daemon-name'
```

You can view the data in Postgres DB using link http://localhost:3001/
You can view Dagster UI using link http://localhost:3000/


