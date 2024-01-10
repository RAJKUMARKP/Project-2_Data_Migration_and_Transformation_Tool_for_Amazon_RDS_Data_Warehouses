import requests
import zipfile
import os
import boto3
from botocore.exceptions import NoCredentialsError
import csv
import json
import pandas as pd
import mysql.connector


#Download Zip File from the site
def download_zip_file(url, filename):
    r = requests.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
    if r.status_code == 200:
        with open(filename, 'wb') as f:
            r.raw.decode_content = True
            f.write(r.content)
            print('Zip File Downloading Completed')

url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
filename = url.split('/')[-1]
download_zip_file(url, filename)


#Extract Zip file and store in a folder
def extract_zip_file(currentpath, extractpath):
    if not os.path.exists(os.path.join(currentpath, extractpath)):
        os.makedirs(extractpath)
    if os.path.isdir(currentpath):
        for file in os.listdir(currentpath):
            if file.endswith('.zip'):
                with zipfile.ZipFile(os.path.join(currentpath, file)) as z:
                    z.extractall(os.path.join(extractpath))
                    print('UnZipping File Process Completed')
    else:
        print('Directory does not exist')

extract_zip_file("F:\Raj\Project-2_Data_Migration_and_Transformation", "Extracted_Files")


#Load extracted each json files into dict
def load_files_into_dict(jsonfilespath):
    i = 1
    files_to_upload = {}
    for file_name in os.listdir(jsonfilespath):
        files_to_upload.update({i: file_name})
        i = i + 1
    return files_to_upload

myprojectpath = "F:\Raj\Project-2_Data_Migration_and_Transformation"
extractpath = "Extracted_Files"
dict_list_of_files = load_files_into_dict(os.path.join(myprojectpath, extractpath))


#Extract required data from each JSON files and save as a CSV file
def extract_data(jsonFiles, dict_list_of_files, path):
    flag = 0
    s = {}
    for file_name in dict_list_of_files.values():
        local_file_path = jsonFiles + '/' + file_name
        get_json_size = os.path.getsize(local_file_path)
        if get_json_size != 2:
            try:
                with open(local_file_path,'r', encoding='utf-8') as f:
                    jsonFileData = json.load(f)
                entity_key = jsonFileData['cik']
                entity_name = jsonFileData['entityName']
                entity_dei_common_stock = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'label']
                entity_dei_common_stock_desc = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'description']
                entity_dei_common_stock_units = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'units']['shares']
                column_name = ["cik", "entity_name", "stock_name", "stock_desc", "end_date", "value", "accn_num", "f_year", "fp_quarter", "form_num", "filed_date", "frame_details"]
                with open(path, 'a', newline='') as fp:
                    writer = csv.DictWriter(fp, fieldnames=column_name)
                    if flag == 0:
                        writer.writeheader()
                    for i in entity_dei_common_stock_units:
                        end = i.get('end')
                        val = i.get('val')
                        accn = i.get('accn')
                        fy = i.get('fy')
                        fp = i.get('fp')
                        form = i.get('form')
                        filed = i.get('filed')
                        frame = i.get('frame')
                        s.update([("cik", entity_key), ("entity_name", entity_name),
                                    ("stock_name", entity_dei_common_stock),
                                    ("stock_desc", entity_dei_common_stock_desc),
                                    ("end_date", end), ("value", val), ("accn_num", accn),
                                    ("f_year", fy), ("fp_quarter", fp),
                                    ("form_num", form), ("filed_date", filed), ("frame_details", frame)])
                        writer.writerow(s)
                flag = flag + 1
            except KeyError:
                print("Key not found in json file")

extract_data(os.path.join(myprojectpath, extractpath), dict_list_of_files, os.path.join(myprojectpath, 'consolidated.csv'))


#Convert extracted CSV to JSON file
def convert_csv_into_json(file, json_file):
    data = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        field = reader.fieldnames
        for row in reader:
            data.extend([{field[i]:row[field[i]] for i in range(len(field))}])
           
    with open(json_file, "w", encoding='utf-8') as f:
        f.write(json.dumps(data, sort_keys=False, indent=4))

convert_csv_into_json(os.path.join(myprojectpath, 'consolidated.csv'), os.path.join(myprojectpath, 'consolidated.json'))


#Load extracted each json files into dict
def load_jsonfile_into_dict(jsonfilespath):
    i = 1
    jsonfile_to_upload = {}
    for file_name in os.listdir(jsonfilespath):
        if file_name.endswith('.json'):
            jsonfile_to_upload.update({i: file_name})
        i = i + 1
    return jsonfile_to_upload

dict_list_of_jsonfile = load_jsonfile_into_dict(myprojectpath)


#Upload loaded dict of json files into Amazon S3 Bucket
def upload_files_to_awss3(jsonfilespath, list_of_jsonfile_dict, bucket, access_key, secret_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    for file_name in list_of_jsonfile_dict.values():
        try:
            local_file_path = jsonfilespath + '/' + file_name
            s3_key = file_name
            s3.upload_file(local_file_path, bucket, 'Data Migration and Transformation/{}'.format(s3_key))
            print("Upload Successful")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

upload_files_to_awss3(myprojectpath, dict_list_of_jsonfile, "Bucketname", "Acesskey", "Secretkey")


#Load data from S3 into Amazon RDS
def load_data_to_rds(json_file_name, bucket, access_key, secret_key):
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    json_object = s3_client.get_object(Bucket=bucket,Key=json_file_name)
    jsonFileReader = json_object['Body'].read()
    jsonDict = json.loads(jsonFileReader)
    dfs = []
    for item in (jsonDict):
         dfs.append(item.values())
    df = pd.DataFrame.from_dict(dfs)

    connection = mysql.connector.connect(host = endpoint, user = username, passwd = password, db = database)
    cursor = connection.cursor()

    drop_query='''DROP TABLE IF EXISTS rds.stockdetails'''
    cursor.execute(drop_query)
    connection.commit()

    create_query ='''CREATE TABLE IF NOT EXISTS rds.stockdetails(cik BIGINT,
                                                            entity_name VARCHAR(100),
                                                            stock_name VARCHAR(100),
                                                            stock_desc TEXT,
                                                            end_date DATE,
                                                            value BIGINT,
                                                            accn_num VARCHAR(100),
                                                            f_year YEAR,
                                                            fp_quarter VARCHAR(10),
                                                            form_num VARCHAR(10),
                                                            filed_date DATE,
                                                            frame_details VARCHAR(20)'''
    
    cursor.execute(create_query)
    connection.commit()


    for index,row in df.iterrows():
        pymysql_insert ='''INSERT INTO rds.stockdetails(cik,
                                                    entity_name,
                                                    stock_name,
                                                    stock_desc,
                                                    end_date,
                                                    value,
                                                    accn_num,
                                                    f_year,
                                                    fp_quarter,
                                                    form_num,
                                                    filed_date,
                                                    frame_details)
                                                    
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        values=(row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11])
                 
        cursor.executemany(pymysql_insert, values)
        connection.commit()
        cursor.close
        connection.close
        print(cursor.rowcount, "record inserted successfully into  table")

load_data_to_rds("json_filename", "Bucketname", "Accesskey", "Secretkey")