import pandas as pd
from pyspark.sql.functions import udf

def create_valid_ports():
    """
    Use the i94 labels SAS file to create valid port labels and remove invalid ones from i94 dataset
    """
    # Create list of valid ports
    with open('I94_SAS_Labels_Descriptions.SAS') as f:
        lines = f.readlines()
    ports = []
    for line in lines[302:962]:
        ports.append(line.strip())
    ports_split = []    
    for port in ports:
        ports_split.append(port.split("="))
    port_codes = []
    for code in ports_split:
        port_codes.append(code[0].replace("'","").strip())
    port_locations = []
    for location in ports_split:
        port_locations.append(location[1].replace("'","").strip())
    port_cities = []
    for city in port_locations:
        port_cities.append(city.split(",")[0])
    port_states = []
    for state in port_locations:
        port_states.append(state.split(",")[-1])

    port_location_df = pd.DataFrame({"port_code" : port_codes, "port_city": port_cities, "port_state": port_states})

    return port_location_df

def clean_i94(i94_df):
    """
    Filter out invalid ports from i94 data and return cleaned dataframe
    """
    # Check size before and after cleaning
    # Clean dataframe of invalid ports
    port_location_df = create_valid_ports()
    invalid_ports = list(set(port_location_df[port_location_df["port_city"] == port_location_df["port_state"]]["port_code"].values))
    print(f"number of rows in i94 before clenaing: {i94_df.count()}")
    filtered_i94_df = i94_df[~i94_df["i94port"].isin(invalid_ports)]
    print(f"number of rows in i94 after clenaing: {filtered_i94_df.count()}")
    # Drop empty rows in i94 dataframe
    dropped_columns = ("insnum", "entdepu", "occup", "visapost")
    dropped_i94_df = filtered_i94_df.drop(*dropped_columns)
    
    return dropped_i94_df

@udf()
def get_port(city):
    """
    Add valid port based on city data
    """
    port_location_df = create_valid_ports()
    for port_city in port_location_df.port_city:
        if city.lower() in port_city.lower():
            return port_location_df[port_location_df["port_city"]==port_city]["port_code"].values[0]
        
def add_port(df):
    """
    Add valid ports to table to allow joining of tables
    """
    port_location_df = create_valid_ports()
    df = df.withColumn("i94port", get_port(df.City))
    new_df = df.filter(df.i94port != 'null')
    return new_df

    
def clean_table(df):
    """
    Drop empty rows in dataframe
    """
    print(f"number of rows in table before removing empty rows: {df.count()}")    
    clean_df = df.dropna()
    print(f"number of rows in table after removing empty rows: {clean_df.count()}")
    return clean_df