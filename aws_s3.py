import boto3
import pandas as pd
import math, os
from io import StringIO

from khepri_utils import basic, sql

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# %% READING DATA FROM S3
#TAKES AN EXCEL FILE IN S3 AND BREAKS INTO ~500mb size CSVs by default
def split_s3_xlsx_to_csv(payload, bucket, key, maxChunkSizeMb = 500):
    

    fileNames=[]
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    status = obj.get("ResponseMetadata", {}).get("HTTPStatusCode")
    fileSizeMb = obj.get("ContentLength", {}) / (10**6)
    print(status, fileSizeMb)
    
    data = pd.DataFrame()
    if status == 200:
        name, ext = os.path.splitext(key)
        print("Name = ", name)
        print('EXT = ', ext)
        #GET LINE COUNT OF FILE

        if ext.lower() == '.xlsx':
            recordCount = len(
                pd.read_excel(
                    io = obj.get("Body").read(), 
                    sheet_name = 0,
                    dtype= str,
                    na_filter= False,
                    engine = 'openpyxl',
                    usecols = [0]
                    )
                )
            
        if ext.lower() == '.xls':
            recordCount = len(
                pd.read_excel(
                    io = obj.get("Body").read(), 
                    sheet_name = 0,
                    dtype= str, 
                    na_filter= False,
                    engine = 'xlrd',
                    usecols = [0]
                    )
            )
            
        chunkCount = math.ceil(fileSizeMb/maxChunkSizeMb)
        print('chunkCount',chunkCount)
        numRows = math.ceil(recordCount/chunkCount)
        print('Records Per Chunk',numRows)
        headers = []
        if ext.lower() == '.xlsx':

            for i in range(chunkCount):
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                print(i)
                skipRows = i*numRows
                if i == 0:
                    data = pd.read_excel(
                        io = obj.get("Body").read(), 
                        sheet_name = 0,
                        dtype= str,
                        na_filter= False,
                        engine = 'openpyxl',
                        skiprows = skipRows,
                        nrows = numRows
                            )
                    headers = sql.normalize_col_names(data.columns.to_list())
                    data.columns = headers
                else:
                    data = pd.read_excel(
                        io = obj.get("Body").read(), 
                        sheet_name = 0,
                        header = None,
                        names = headers,
                        dtype= str,
                        na_filter= False,
                        engine = 'openpyxl',
                        skiprows = skipRows+1,
                        nrows = numRows
                            )
                    
                print(data)
                folder = '/'.join(key.split('/')[:-1])
                fileName = os.path.splitext(key.split('/')[-1])[0]
                extNum = str(i).rjust(3,'0')
                csv_buffer = StringIO()
                data.to_csv(csv_buffer,index=False)
                objKey = f'{folder}/{payload["log meta data"]["log_id"]}_{fileName}_{extNum}.csv'
                print(objKey)
                fileNames.append(objKey)
                s3_resource.Object(bucket, objKey).put(Body=csv_buffer.getvalue())
            
        if ext.lower() == '.xls':
            for i in range(chunkCount):
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                print(i)
                skipRows = i*numRows
                data = pd.read_excel(
                    io = obj.get("Body").read(), 
                    sheet_name = 0,
                    dtype= str,
                    na_filter= False,
                    engine = 'xlrd',
                    skiprows = skipRows,
                    nrows = recordCount
                        )
                print(data)        
    return fileNames

def s3_to_df(bucket, objectKey, delimiter = ','):
    metadata = s3_client.head_object(Bucket=bucket, Key=objectKey)
    obj = s3_client.get_object(Bucket=bucket, Key=objectKey)
    status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
    fileSizeMb = metadata.get("ContentLength", {})/(10**6)
    
    print(status, f'{fileSizeMb} MB')
    data = pd.DataFrame()
    
    if status == 200:
        # for k,v in metadata.items():
        #     print('KEY',k,':',v)
        name, ext = os.path.splitext(objectKey)
        print("Name = ", name)
        print('EXT = ', ext)
        if ext.lower() == '.csv':
            if delimiter == '':
                delimiter = ','
            data = pd.read_csv(
                    obj.get("Body"), 
                    dtype= str, 
                    sep = delimiter,
                    na_filter= False,
                    low_memory=False
                )             
                
            # print('inner', data)
                
        elif ext.lower() == '.xlsx':
            data = pd.read_excel(
                io = obj.get("Body").read(), 
                sheet_name = 0,
                dtype= str,
                na_filter= False,
                engine = 'openpyxl'
                )
            
        elif ext.lower() == '.xls':
            data = pd.read_excel(
                io = obj.get("Body").read(), 
                sheet_name = 0,
                dtype= str, 
                na_filter= False,
                engine = 'xlrd'
                )

        else:
            if delimiter == '':
                print('NO DELIMITER VALUE')
                return data
                
            else:
                data = pd.read_csv(
                        obj.get("Body"), 
                        dtype= str, 
                        sep = delimiter, 
                        low_memory=False,
                        na_filter= False
                    )
    
        return data  
    else:
        return 'FILE NOT FOUND'

def get_s3_file_meta_data(bucket, key, delimiter = ',', columnMap = '',nRows = 1000000, inferDTypes = True):
    
    metadata = s3_client.head_object(Bucket=bucket, Key=key)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    
    status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
    fileSizeMb = metadata.get("ContentLength", {})/(10**6)
    print(status, fileSizeMb)
    data = pd.DataFrame()
    
    if status == 200:    
        # for k,v in metadata.items():
        #     print('KEY',k,':',v)
        name, ext = os.path.splitext(key)
        # print("Name = ", name)
        # print('EXT = ', ext)
        # print()
            
        if ext.lower() == '.csv':
            if isinstance(columnMap,list):
                data = pd.read_csv(
                    obj.get("Body"), 
                    header = None,
                    names = columnMap,
                    dtype= str, 
                    sep = delimiter, 
                    na_filter= False,
                    low_memory=False,
                    nrows = nRows
                    )
            elif isinstance(columnMap,dict):
                 data = pd.read_csv(
                    obj.get("Body"), 
                    dtype= str, 
                    sep = delimiter, 
                     na_filter= False,
                    low_memory=False,
                    nrows = nRows
                )       
                # CODE HERE TO OVERWRITE COLUMNS BASED ON MAP
            else:
                print('NO COLUMN MAP')
                data = pd.read_csv(
                    obj.get("Body"), 
                    dtype= str, 
                    na_filter= False,
                    low_memory=False,
                    nrows = nRows
                )
        elif ext.lower() == '.xlsx':
            data = pd.read_excel(
                io = obj.get("Body").read(), 
                sheet_name = 0,
                dtype= str,
                na_filter= False,
                engine = 'openpyxl',
                nrows = nRows
                )
        elif ext.lower() == '.xls':
            data = pd.read_excel(
                io = obj.get("Body").read(), 
                sheet_name = 0,
                dtype= str, 
                na_filter= False,
                engine = 'xlrd',
                nrows = nRows
                )

        else:
            if delimiter == '':
                print('NO DELIMITER VALUE')
                return data
            else:
                print('COLUMN MAP', type(columnMap))
                if isinstance(columnMap,list):
                    data = pd.read_csv(
                        obj.get("Body"), 
                        header = None,
                        names = columnMap,
                        dtype= str, 
                        sep = delimiter,
                        na_filter= False,
                        low_memory=False,
                        nrows = nRows
                    )
                elif isinstance(columnMap,dict):
                     data = pd.read_csv(
                         obj.get("Body"),
                         dtype= str,
                         sep = delimiter,
                         low_memory=False,
                         na_filter= False,
                         nrows = nRows
                    )                        
                else:
                    print('NO COLUMN MAP')
                    data = pd.read_csv(
                        obj.get("Body"), 
                        dtype= str, 
                        sep = delimiter, 
                        low_memory=False,
                        na_filter= False,
                        nrows = nRows
                    )
        print('DATA EXTRACTED')
        data.columns = sql.normalize_col_names(data.columns)
        if inferDTypes:
            data = basic.autoConvert(data)
        print('DATA CONVERTED')
        
        # print(data.info(verbose=True))

    return data

#CONVERTS S3 FILE INTO STANDARD CSV
def format_file(bucket, objectKey, log_id = '', delimiter = ',', columnMap = '',lineterminator = ''):
    name, ext = os.path.splitext(objectKey)
    data = s3_to_df(bucket = bucket, objectKey = objectKey, delimiter = delimiter)   
    s3FileName = f"""{'/'.join(name.split('/')[:-1])}/{log_id}_{name.split('/')[-1]}.csv"""
    # s3FileName ='/'.join(name.split('/')[:-2])}{log_id}_{name.split[:-2]}.csv"
    print(s3FileName)
    print('DATA EXTRACTED')
    # print({list(data.columns)[i]:'' for i in range(len(list(data.columns)))})
    # print('-'*50)
    
    if isinstance(columnMap,dict):
        columnMap = {k.upper():v.upper() for k,v in columnMap.items()}
        data.columns = [c.upper() for c in data.columns]
        data.rename(columns=columnMap, inplace=True)        
        
    if isinstance(columnMap,list):
        data.columns = columnMap
        
    data.columns = sql.normalize_col_names(data.columns)
    # print(data.columns)
    print('DATA CONVERTED')
    
    send_to_s3(data = data, bucket = bucket, s3FileName = s3FileName)
    print('DATA SENT TO S3')
    # print(data)
    return s3FileName, data.columns.to_list(), data

# %% WRITING DATA FROM S3
def send_to_s3(data, bucket, s3FileName, sheet_name = 'Sheet1', delimiter = ''):
    #NEEDS TESTING
    name, ext = os.path.splitext(s3FileName)
    
    if delimiter == '' and ext.lower() not in ['.csv','.xlsx']:

        if ext.lower() == '.json':
            json_data = json.dumps(data)
        
            # Upload JSON string to S3
            response = s3_client.put_object(Bucket=bucket, Key=s3FileName, Body=json_data)
        else:
            obj = s3_resource.Object(bucket, s3FileName)
            response = obj.put(Body = bytes(data, 'utf-8'))
           
    else:
        if delimiter != '':
            with io.StringIO() as file_buffer:
                data.to_csv(
                    file_buffer,
                    sep = delimiter,
                    index=False
                )
                response = s3_client.put_object(
                    Bucket = bucket, 
                    Key = s3FileName, 
                    Body = file_buffer.getvalue()
                )
                
        else:
            
            if ext == '.csv':
                with io.StringIO() as file_buffer:
                    data.to_csv(file_buffer, index=False)
                    response = s3_client.put_object(
                    Bucket=bucket, 
                    Key=s3FileName, 
                    Body=file_buffer.getvalue()
                    )
            
            if ext == '.xlsx':
                with io.BytesIO() as stream:
                    with pd.ExcelWriter(stream) as writer:
                        data.to_excel(writer, sheet_name=sheet_name, index=False)
                        writer.save()
                        stream.seek(0)
                        response = s3_client.put_object(
                            Bucket=bucket, 
                            Key=s3FileName, 
                            Body=stream.read()
                        )              

    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    return status