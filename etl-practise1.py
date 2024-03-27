import requests #pull request from API
import pandas as pd
from sqlalchemy import create_engine

def extract()-> dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in california {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]


def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    # disk_engine = create_engine('sqlite:///my_lite_store.db')
    # df.to_sql('cal_uni', disk_engine, if_exists='replace')
    try:
        # Create SQLite engine
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        
        # Load DataFrame into SQLite table
        df.to_sql('cal_uni', disk_engine, if_exists='replace', index=False)
        
        # Verification step: Print existing table names to verify
        print("Existing tables:", disk_engine.table_names())
    except Exception as e:
        print(f"An error occurred: {e}")


data = extract()
df = transform(data)
load(df)


# from sqlalchemy import create_engine
# import pandas as pd
# for reading the data from table
# Create an engine
engine = create_engine('sqlite:///my_lite_store.db')

# Query the cal_uni table and load into a DataFrame
query = "SELECT * FROM cal_uni"
df = pd.read_sql_query(query, engine)

# Display the DataFrame
print(df)
