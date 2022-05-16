# ETL_Temperature_Countries_Cities
This is an ETL process to automate the Earth Surface Temperature Data from www.kaggle.com. The link of the dataset is https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data

- Technologies listed as below were used to implement the ETL process:
1. Programming Language: Python.
2. Database System: PostgreSQL.
3. Database Took Kit: SQLAlchemy Core.
4. Packages: Pandas, SQLAlchemy, GeoAlchemy2, json.

- General introduction:
  The purpose of this ETL is generate a process which can produce the linked data between the TemperatureByCountry and the correspnding TemperatureByMajorCity based on their common fields: date and country. As the subject of temperature information in each source dataset is either city or country but not both, so this ETL is needed to improve the data analysis by linking the temperature of country and  its major cities.


