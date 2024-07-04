#libraries
import pandas as pd
import psycopg2
import requests
from data_transformation import transform_dataframe
from web_scraper_update import scraper_function
import glob
import os
from datetime import datetime





#DAG:UPDATE
def insert_data_into_stage_dag_update(**kwargs):
    df = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_new_entries.csv")

    conn = psycopg2.connect(
        dbname='real_estate_project',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO real_estate.staging_table (online_id, webpage,property_type,delivery_time,energy_consumption,energy_class,sqm_plot,sqm_property,postal_code,price,no_rooms,no_floor,publisher_type,apartment_house_type,construction_year,publisher_name,german_state,city) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (online_id) DO NOTHING;",
            (row['online_id'], row['property_webpage'],row['final_category_of_home'],row['delivery_time'],row['final_energy_consumption'],
             row['final_energy_class'],row['final_plot_area'],row['final_property_area'],row['final_postal_code'],row['final_property_price'],row['final_property_rooms'],row['final_property_floor'],row['final_type_provider_property'],row['property_type'],row['final_construction_year'],row['final_offerer_name'],row["german_state"],row["final_city"])
        )
        
    conn.commit()
    cursor.close()
    conn.close()

def insert_data_into_table_all_current_webpages_update(**kwargs):
    current_webpages_df = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/all_current_pages.csv")
    conn = psycopg2.connect(
        dbname='real_estate_project',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    for index, row in current_webpages_df.iterrows():
        cursor.execute(
            "INSERT INTO real_estate.all_current_webpages (current_webpage) VALUES (%s) ON CONFLICT DO NOTHING",
            (row['current_webpage'],)
        )
        
    conn.commit()
    cursor.close()
    conn.close()

def getting_data_update(**kwargs):
    conn = psycopg2.connect(
        dbname='real_estate_project',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    query = "select distinct webpage from real_estate.fact_table;"
    cursor.execute(query)
    existing_webpages = cursor.fetchall()  
    # Retrieve column names
    #colnames = [desc[0] for desc in cursor.description]
    
    #conn.commit()
    cursor.close()
    conn.close()
    webpages = [row[0] for row in existing_webpages]
    return webpages  







#Functions inside the dag update
def webpages_before_update():
    existing_webpages_list = getting_data_update()
    return existing_webpages_list


#Edo kano export tis existing wepages oste na kontrolaro , an diavazei sosta tis idi existing webpages
def export_webpages_before_update(**kwargs):
    existing_webpages_before_update_df = pd.DataFrame(webpages_before_update(), columns=['webpage'])
    existing_webpages_before_update_df.to_csv("/opt/airflow/dags/csvs/temporary_csvs/existing_webpages_before_update.csv", index=False)
    current_date = datetime.now().strftime('%Y-%m-%d')
    existing_webpages_before_update_file_name = f"/opt/airflow/dags/csvs/back_up_csvs/existing_webpages_before_update/existing_webpages_before_update_{current_date}.csv"
    existing_webpages_before_update_df.to_csv(existing_webpages_before_update_file_name, index=False)


#Edo tha treksei o scraper kai thelo na kratiso oles tis selides
def run_web_scraper_update(**kwargs):
    iparxouses_selides_lista = webpages_before_update()
    all_current_pages_lista,epistrofi_scraper_iparxouses_selides_lista,new_entries_lista = scraper_function(iparxouses_selides_lista)
    all_current_pages_df = pd.DataFrame(all_current_pages_lista, columns=['current_webpage'])
    current_date = datetime.now().strftime('%Y-%m-%d')
    new_entries_file_name = f"/opt/airflow/dags/csvs/back_up_csvs/new_entries/new_entries_{current_date}.csv"
    all_current_pages_file_name = f"/opt/airflow/dags/csvs/back_up_csvs/all_current_webpages/all_current_pages_{current_date}.csv"
    new_entries_df = pd.DataFrame(new_entries_lista) 
    new_entries_df.to_csv("/opt/airflow/dags/csvs/temporary_csvs/new_entries.csv", index=False)
    new_entries_df.to_csv(new_entries_file_name, index=False)
    all_current_pages_df.to_csv("/opt/airflow/dags/csvs/temporary_csvs/all_current_pages.csv", index=False)
    all_current_pages_df.to_csv(all_current_pages_file_name, index=False)



def transform_new_entries_df_update():
    new_entries_dataframe = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/new_entries.csv")
    transformed_new_entries_dataframe = transform_dataframe(new_entries_dataframe)
    transformed_new_entries_dataframe.to_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_new_entries.csv", index=False)


def send_discord_message_update(webhook_url, message):
    data = {
        "content": message
    }
    response = requests.post(webhook_url, json=data)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
def cleanup_csvs_created_under_run_update():
    try:
        new_entries_path = "/opt/airflow/dags/csvs/temporary_csvs/new_entries.csv"
        os.remove(new_entries_path)
    except FileNotFoundError:pass
    try:
        transformed_new_entries_path = "/opt/airflow/dags/csvs/temporary_csvs/transformed_new_entries.csv"
        os.remove(transformed_new_entries_path)
    except FileNotFoundError:pass
    try:
        all_current_pages_path = "/opt/airflow/dags/csvs/temporary_csvs/all_current_pages.csv"
        os.remove(all_current_pages_path)
    except FileNotFoundError:pass
    try:
        existing_webpages_path = "/opt/airflow/dags/csvs/temporary_csvs/existing_webpages_before_update.csv"
        os.remove(existing_webpages_path)
    except FileNotFoundError:pass





#Initialization

#for table german_geography
def insert_data_into_german_geography_initialization(**kwargs):
    transformed_german_geography_dataframe = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_german_geography_initialization.csv")

    conn = psycopg2.connect(
        dbname='real_estate_project',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    for index, row in transformed_german_geography_dataframe.iterrows():
        cursor.execute(
            "INSERT INTO real_estate.dim_german_geography (postal_code, lat,lng,german_state,city) VALUES (%s, %s, %s,%s,%s) ON CONFLICT (postal_code, lat, lng, german_state,city) DO NOTHING;",
            (row['postal_code'], row['lat'],row['lng'],row['german_state'],row['city'])
        )
        
    conn.commit()
    cursor.close()
    conn.close()

def transform_german_geography(postcodes,geocoord):
    geocoord = geocoord.rename(columns={'Unnamed: 0': 'Plz'})
    # Replace 'Schlewig-Holstein' with 'Schleswig-Holstein' in the 'Bundesland' column
    postcodes['Bundesland'] = postcodes['Bundesland'].replace('Schlewig-Holstein', 'Schleswig-Holstein')

    # Convert all values in the 'Ort' and 'Bundesland' columns to lowercase
    postcodes['Ort'] = postcodes['Ort'].str.lower()
    postcodes['Bundesland'] = postcodes['Bundesland'].str.lower()

    # Group by 'Plz' and 'Bundesland', and concatenate 'Ort'
    grouped = postcodes.groupby(['Plz', 'Bundesland']).agg({
        'Ort': ', '.join
    }).reset_index()

    # Merge the geocoord DataFrame with the grouped DataFrame on 'Plz'
    result = pd.merge(geocoord, grouped, on='Plz', how='inner')
    result = result.rename(columns={'Plz': 'postal_code', 'Ort': 'city','Bundesland': 'german_state'})
    return result

def transformation_german_geography_initialization(**context):
    df_postcodes = pd.read_csv("/opt/airflow/dags/csvs/initial_csvs/german_postcodes.csv", delimiter=';')
    df_plz_geocoord = pd.read_csv("/opt/airflow/dags/csvs/initial_csvs/plz_geocoord.csv")
    df_result = transform_german_geography(df_postcodes,df_plz_geocoord)
    df_result.to_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_german_geography_initialization.csv", index=False)

#for the other tables

def merge_csv_files_initialization(directory,file_pattern="output*.csv", delimiter=','):
    """
    Merge multiple CSV files into a single CSV file.

    Parameters:
    - output_file: str, the name of the output merged CSV file.
    - file_pattern: str, the pattern to match the input CSV files (default is "file*.csv").
    - delimiter: str, the delimiter used in the CSV files (default is ',').
    """
    
    full_file_pattern = os.path.join(directory,file_pattern)
    # Find all files matching the pattern
    csv_files = glob.glob(full_file_pattern)

    # Initialize an empty list to store DataFrames
    dataframes = pd.DataFrame()

    # Loop through the list of files and read each one into a DataFrame
    for file in csv_files:
        df = pd.read_csv(file, delimiter=delimiter)
        dataframes = pd.concat([dataframes,df],ignore_index=True)

    return dataframes

def insert_data_into_stage_initialization(**kwargs):
    df_to_be_inserted_initialization = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_dataframe_initialization.csv")
    conn = psycopg2.connect(
        dbname='real_estate_project',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    cursor = conn.cursor()
    
    for index, row in df_to_be_inserted_initialization.iterrows():
        cursor.execute(
            "INSERT INTO real_estate.staging_table (online_id, webpage,property_type,delivery_time,energy_consumption,energy_class,sqm_plot,sqm_property,postal_code,price,no_rooms,no_floor,publisher_type,apartment_house_type,construction_year,publisher_name,german_state,city) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (online_id) DO NOTHING;",
            (row['online_id'], row['property_webpage'],row['final_category_of_home'],row['delivery_time'],row['final_energy_consumption'],
             row['final_energy_class'],row['final_plot_area'],row['final_property_area'],row['final_postal_code'],row['final_property_price'],row['final_property_rooms'],row['final_property_floor'],row['final_type_provider_property'],row['property_type'],row['final_construction_year'],row['final_offerer_name'],row["german_state"],row["final_city"])
        )
        
    conn.commit()
    cursor.close()
    conn.close()


#FUNCTIONS INSIDE THE DAG INITILIAZATION_TESTING
# Function to read SQL file
def create_directories_exchange_files_initialization():
    os.makedirs('/opt/airflow/dags/csvs/temporary_csvs', exist_ok=True)
    os.makedirs('/opt/airflow/dags/csvs/back_up_csvs', exist_ok=True)

def read_sql_file(filepath):
    with open(filepath, 'r') as file:
        return file.read()


def read_csv_initialization():
    df_merged_initialization = merge_csv_files_initialization('/opt/airflow/dags/csvs/initial_csvs')
    #df_merged_initialization.to_csv("/opt/airflow/dags/csvs/initial_csvs/df_merged_initialization.csv", index=False)
    df_merged_initialization.to_csv("/opt/airflow/dags/csvs/temporary_csvs/df_merged_initialization.csv", index=False)


def transform_dataframe_initialization(**context):
    untransformed_dataframe_initialization = pd.read_csv("/opt/airflow/dags/csvs/temporary_csvs/df_merged_initialization.csv")
    transformed_dataframe_initialization = transform_dataframe(untransformed_dataframe_initialization)
    transformed_dataframe_initialization.to_csv("/opt/airflow/dags/csvs/temporary_csvs/transformed_dataframe_initialization.csv", index=False)

def cleanup_csvs_created_under_run_initialization():
    transformed_dataframe_initialization_path = "/opt/airflow/dags/csvs/temporary_csvs/transformed_dataframe_initialization.csv"
    df_merged_initialization_path = "/opt/airflow/dags/csvs/temporary_csvs/df_merged_initialization.csv"
    transformed_german_geography_initialization_path = "/opt/airflow/dags/csvs/temporary_csvs/transformed_german_geography_initialization.csv"
    os.remove(transformed_dataframe_initialization_path)
    os.remove(df_merged_initialization_path)
    os.remove(transformed_german_geography_initialization_path)

def send_discord_message_initialization(webhook_url, message):
    data = {
        "content": message
    }
    response = requests.post(webhook_url, json=data)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    

