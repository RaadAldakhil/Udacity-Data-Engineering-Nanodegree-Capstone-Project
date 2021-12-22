# Quality checks here

# Check if table exists
def table_exists(df):
    """
    Checks if the dataframe that was entered is exists
    """
    return df is not None

# Check if table is empty
def table_empty(df):
    """
    Checks if the dataframe that was entered contains any rows
    """
    return df.count() != 0

def data_quality_check(df):
    """
    Check if the dataframes exists and contains any data 
    """
    if table_exists(df):
        print("Data quality check #1 passed, fact and dimension table exist\n")
    else:
        print("Data quality check #1 failed, table is missing\n")
    if table_empty(df):
        print("Data quality check #2 passed, fact and dimension table not empty\n")
    else:
        print("Data quality check #2 failed, table is empty\n")