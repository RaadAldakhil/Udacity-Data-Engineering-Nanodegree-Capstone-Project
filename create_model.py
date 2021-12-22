def write_immigration_data(cleaned_i94_df):
    """
    Write immigration data to parquet
    """
    immigration_table = cleaned_i94_df.select(["cicid" ,"i94yr" ,"i94mon" ,"i94cit" ,"i94res" ,"i94port" ,"arrdate" \
                                               ,"i94mode" ,"i94addr" ,"depdate" ,"i94bir" ,"i94visa" ,"count" ,"dtadfile" \
                                               ,"entdepa" ,"entdepd" ,"matflag" ,"biryear" ,"dtaddto" ,"gender" ,"airline" \
                                               ,"admnum" ,"fltno" ,"visatype"])
    immigration_table.write.mode("append").partitionBy("i94port").parquet("./output/immigration_table.parquet")
    
def write_temperature_data(cleaned_temperature_df):
    """
    Write temperature data to parquet
    """
    temperature_table = cleaned_temperature_df.select(["dt" ,"AverageTemperature" ,"AverageTemperatureUncertainty" ,"City" \
                                                       ,"Country" ,"Latitude" ,"Longitude"])
    temperature_table.write.mode("append").partitionBy("Country").parquet("./output/temperature_table.parquet")
    
def write_demographics_data(fixed_cleaned_demographics_df):
    """
    Write demographics data to parquet
    """
    demographics_table = fixed_cleaned_demographics_df.select(["City" ,"State" ,"median_age" ,"male_population" \
                                                               ,"female_population" ,"total_population" \
                                                               ,"number_of_veterans" ,"foreign_born" \
                                                               ,"average_household_size" ,"state_code" ,"Race" ,"Count"])
    demographics_table.write.mode("append").partitionBy("state_code").parquet("./output/demographics_table.parquet")
    
def write_airport_code_data(cleaned_airport_code_df):
    """
    Write demographics data to parquet
    """
    airport_code_table = cleaned_airport_code_df.select(["ident" ,"type" ,"name" ,"elevation_ft" ,"continent" ,"iso_country" ,"iso_region" ,"municipality" ,"gps_code" ,"iata_code" ,"local_code" ,"coordinates"])
    airport_code_table.write.mode("append").partitionBy("iata_code").parquet("./output/airport_code_table.parquet")
    
def create_model(cleaned_i94_df, cleaned_temperature_df, fixed_cleaned_demographics_df, cleaned_airport_code_df):
    """
    Writes all models to paraquet files
    """
    write_immigration_data(cleaned_i94_df)
    write_temperature_data(cleaned_temperature_df)
    write_demographics_data(fixed_cleaned_demographics_df)
    write_airport_code_data(cleaned_airport_code_df)