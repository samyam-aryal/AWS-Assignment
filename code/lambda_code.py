import json
import boto3
import psycopg2
import ast
import pandas as pd
import http.client
import logging
import os
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    #Setup
    s3_client = boto3.client("s3")
    conn = http.client.HTTPSConnection("free-nba.p.rapidapi.com")
    
    
    #API Details & Implementation
    headers = {'X-RapidAPI-Key': "5db8c6164cmshb1ba27608285dbcp16367fjsn2803e1036525",
    'X-RapidAPI-Host': "free-nba.p.rapidapi.com"}
    
    conn.request("GET", "/players?page=0&per_page=25", headers=headers)
    res = conn.getresponse()
    data = res.read()
    
    a = data.decode("utf-8")
    
    
    
    ############################################################################
    
    
    
    #Load API data as json
    parsed_data = json.loads(a)
    
    json_string = json.dumps(parsed_data)
    data = json.loads(json_string)
    
    
    #Convert to Pandas dataframe
    df = pd.DataFrame(data["data"])
    
    raw_data = df.to_csv(index=False)
    
    bucket_name = "apprentice-training-ml-dev-samyam-raw" #Raw data bucket
    object_key = "raw_data.csv"  #Raw data object
    
    
    #Put raw data to object
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=raw_data,  # Upload the raw data bytes
        ContentType="application/csv"
    )
    
    print("Raw CSV data uploaded to S3 successfully.")
    
    
    
    ############################################################################
    #WORKING WITH DATAFRAMES


    # Drop columns with over 80% missing values
    for col in df.columns:
        if df[col].isnull().mean() > 0.8:
            df.drop(col, axis=1, inplace=True)
            
    #result_df = df.copy()
    dataframe = df.copy()
    
    # Unpack values from 'team' column, which had data as dict, and load them into the dataframe
    expanded_data = pd.json_normalize(dataframe['team'])
    result_dataframe = pd.concat([dataframe, expanded_data], axis=1)
    result_dataframe = result_dataframe.drop(columns=['team'])
    result_dataframe.rename({'full_name':'team_name'}, axis=1, inplace=True)
    result_dataframe.drop('name', axis=1, inplace=True)
    
    
    # Add csv file to destination bucket
    cleaned_file = result_dataframe.to_csv(index=False)
    clean_bucket_name = "apprentice-training-ml-dev-samyam-cleaned"
    clean_destination_key = "cleaned_file.csv"
    
    s3_client.put_object(
        Bucket=clean_bucket_name,
        Key=clean_destination_key,
        Body=cleaned_file,
        ContentType="application/csv"
    )
    
    
    # Fetch cleaned csv from clean bucket
    try:
        response = s3_client.get_object(Bucket=clean_bucket_name,
            Key=clean_destination_key)
        print("Fetch successful")
    except:
        print("Failed to fetch from clean bucket")
        
    #Extract content from response of get call.
    object_content = response['Body'].read().decode('utf-8')

    # Load content into a Pandas DataFrame
    dataframe = pd.read_csv(io.StringIO(object_content))
    
    
    
    ############################################################################
    
    
    # Connect to RDS instance using environment variables
    try:
        dbconn = psycopg2.connect(
            host=os.environ['host'],
            database=os.environ['database'],
            user=os.environ['user'],
            password=os.environ['password']
        )
        print("SUCCESS!")
    except:
        print("Something went Wrong")
    cursor = dbconn.cursor()
    
    # Extract data from the dataframe, and 
    for _, row in result_dataframe.iterrows():
        insert_query = (
            "INSERT INTO etl_samyam_nba_table (first_name, last_name, position, abbreviation, city,"
            "conference, division, team_name)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        )
        values = (
            row['first_name'], row['last_name'], row['position'], row['abbreviation'],
            row['city'], row['conference'], row['division'], row['team_name']
        )
    
        # Assuming you have a database connection named "conn" established
        cursor.execute(insert_query, values)
    dbconn.commit()
    cursor.close()

    logger.info("Raw CSV data uploaded to S3 successfully.")
    logger.info("Data uploaded to RDS successfully.")

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    