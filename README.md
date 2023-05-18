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

Data are retrievied via API requests.</br>

Depending on the number of API requests needed to retrieve the data, a different approach is used to perform the retrieval:

Air quality
1) List of measurement stations
   - API: http://api.gios.gov.pl/pjp-api/rest/station/findAll
   - Tool: Synapse(Pipeline)

2) List of measure points for each station
   - API: https://api.gios.gov.pl/pjp-api/rest/station/sensors/{stationId}
   - Tool: Azure Function App + Synapse(Pipeline)

3) Measurment values for each point
   - API: https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensorId}
   - Tool: Azure Function App + Synapse(Pipeline)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Each station can contain more than one measure point and each measure point requires separtate API request.</br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;In order to retrieve measurement data for all points it's needed to execute more than 700 API calls.</br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;To optimize cost, retrieving and storing data are perfomed via Azure Functions App. App is written in Python.</br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Funcion run is scheduled via Synapse Analytics(Pipeline).</br>


Weather conditions</br>
1) Weather data
   - API: https://danepubliczne.imgw.pl/api/data/synop
   - Tool: Azure Function App + Synapse(Pipeline)
   - Description: Measurment are taken every hour in the UTC time. Example: API request is made at 10:05 UTC and measurment time in the JSON responce is from 9:00 UTC. Function App calls API and stores its response in the JSON format in the data lake. Funtion is triggered every hour via Azure Syanpse(Pipeline).


#### Store and Transform

Raw data are taken from data lake, transformed via Databricks and stored in Delta format.
![data_transformations_databrick_gold](https://github.com/rafalnac/weather-air-quality-poland/assets/98704847/54dc6332-43f7-40a8-a543-3b7c30b8fd0d)

#### Data model

![data_model](https://github.com/rafalnac/weather-air-quality-poland/assets/98704847/060f8956-6b5b-4055-9d0f-c342098cefb1)
