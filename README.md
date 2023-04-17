# The dependence of air quality on weather conditions in Poland

The project aims to track air quality in Poland and its dependence on the weather conditions present at the same time.

### Data archtecture
![data_architecture](https://user-images.githubusercontent.com/98704847/231006425-91daa2cb-e0cf-4c3d-abc1-a920a2fcfdf8.png)

### Data sources
#### Air quality
Air quality data are obtained from API available under https://powietrze.gios.gov.pl/pjp/content/api

#### Weather conditions
Weather conditions data are obtained from the API available under https://danepubliczne.imgw.pl/apiinfo </br>
Note: User is required to read and accept the Terms and Conditions available on the website before using the API.

#### Ingest

Data are retrievied via API requests.

Depending on the number of API requests needed to retrieve the data, a different approach is used to perform the retrieval.



- Air quality(https://powietrze.gios.gov.pl/pjp/content/api)
1) List of measurement stations
API: http://api.gios.gov.pl/pjp-api/rest/station/findAll
Tool: Synapse(Pipeline)

2) List of measure points for each station
API: https://api.gios.gov.pl/pjp-api/rest/station/sensors/{stationId}
Tool: Synapse(Pipeline)

3) Measurment values for each point
API: https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensorId}
Tool: Azure Function App + Synapse(Pipeline)</br>
Each station can contain more than one measure point and each measure point requires separtate API request.
In order to retrieve measurement data for all points it's needed to execute more than 700 API calls.
To optimize cost, retrieving and storing data are perfomed via Azure Functions App. App is written in Python.
Funcion run is scheduled via Synapse Analytics(Pipeline).


- Weather conditions(https://danepubliczne.imgw.pl/apiinfo)
In development


#### Store and Transform
In development

Raw data are taken from data lake and transformed via Databricks and stored in Delta format.

![data_transformations_databricks](https://user-images.githubusercontent.com/98704847/232466850-a6665e00-ca6c-4a39-80f6-c29838c2cb1a.png)

#### Data model

In development...
