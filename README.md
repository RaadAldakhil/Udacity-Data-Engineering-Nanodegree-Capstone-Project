# Capstone Project
This porject aims to study which US cities are most popular for immigration. Providing data on demographics on the arrivals, such as gender, visa types, median ages, etc. As well as providing data on the climates of different cities to study any corelation between temperature and rate of immigrations. And airport data to identify trends in which airlines are most used and departure destinations.As the most recent immigration data is from 2016 while temperature stops at 2013, temperature was reduced to only use averages from 2013 for the most recent data. Spark was used for the ETL pipeline and data stored in parquet for analysis.

## Data Sources
### I94 Immigration Data: 
This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.

### World Temperature Data: 
This dataset came from Kaggle.

### U.S. City Demographic Data: 
This data comes from OpenSoft.

### Airport Code Table: 
This is a simple table of airport codes and corresponding cities.

## Data Model
Immigration data has key identifiers for the rest of the data and can be supplemented with the other tables(temperature, demographics, and airport codes). City and IATA Code identifiers can be used to join tables. A star schema was selected for its simplicity, it allows users to join fact and dimension tables and allows them to analyze the data per user requirments.

| table | columns | description | type |
|---|---|---|---|
| Immigrations | cicid; i94yr; i94mon; i94cit; i94res; i94port; arrdate; i94mode; i94addr; depdate; i94bir; i94visa; count; dtadfile; entdepa; entdepd; matflag; biryear; dtaddto; gender; airline; admnum; fltno; visatype;  | Contains i94 immigration data | Fact Table |
| Temperature | dt; AverageTemperature; AverageTemperatureUncertainty;  City; Country; Latitude; Longitude; i94port; | Contains temperature data | Dimension Table |
| Demographics | City; State; Median Age; Male Population; Female Population; Total Population; Number of Veterans; Foreign-born; Average Household Size; State Code; Race; Count; i94port; | Contains airport data | Dimension Table |
| Airport | ident; type; name; elevation_ft; continent; iso_country; iso_region;  municipality; gps_code; iata_code; local_code; coordinates; | Contains demographicsdata | Dimension Table |

## Data Pipeline
1. Clean i94 of nulls and empty rows and invalid port names (using the i94 SAS file to compare the labels).
2. Clean temperature, demographics, and airport code of null data and drop columns that won't be used.
3. Create fact table using data from immigration file and select relevant columns and write them to a parquet file.
4. Create dimension tables using data from temperature, demographics, and airport code files and select relevant columns and write them to a parquet file.

## Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
* Propose how often the data should be updated and why.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 * The database needed to be accessed by 100+ people.
 
### Rationale:
Using Spark allows for the ability to handle multiple file formats that contians many rows of data. And allows integration with Redshift or increasing nodes should requirments change.

### Data Update Schedule 
This depends on the update cycle of the data itself, in the ase of temperature data, since it is updated monthly, we can adopt a monthly update cycle.

### Scenarios:
- The data was increased by 100x.
    * Using Spark, we can scale up by increasing the number of worker nodes working on the data.
- The data populates a ddashboardashboard that must be updated on a daily basis by 7am every day.
    * Integration with Apache Airflow can be performed to allow a scheduled DAG to query the data everyday at 7am.
- The database needed to be accessed by 100+ people.
    * Data can be migrated to Redshift to allow auto-scaling capabilities to handle the load of increased access by users.
    
### Notes:
Requires updated version of pandas and pyarrow to function.