# Reading from the bronze bucket and writing it to the silver bucket
import pandas as pd
import boto3
from io import BytesIO
import json
import io

#AWS client
s3= boto3.client('s3')

#buckets
bronze_bucket="breta-you-tube-data-pipeline-bronze"
silver_bucket ="breta-you-tube-data-pipeline-silver"

#folder inside the bronze
prefix="raw/"

#encodings
encodings = [
    "utf-8",
    "latin1",
    "cp1252",
    "ISO-8859-1"
]

#get files
response= s3.list_objects_v2(
    Bucket=bronze_bucket,
    Prefix=prefix
)

for obj in response.get("Contents", []):
    key = obj["Key"]

    print(f'reading {key}')

    #skip folders
    if key.endswith('/'):
        continue

    try:
        #READ CSV FILES
        if key.endswith('.csv'):

            df = None

            for encoding in encodings:
                try:
                    s3_object=s3.get_object(
                        Bucket=bronze_bucket,
                        Key=key
                    )

                    content = s3_object["Body"].read().decode(encoding)

                    df.pd.read_csv(
                        io.BytesIO(content),
                        encoding=encoding,
                        encoding_errors="replace",
                        on_bad_lines='skip',
                        engine='python'
                    )

                    print(f'successfully read using {encoding}')

                    break

                except Exception:
                    print(f'failed to read using {encoding}')

            if df is None:
                print(f'skipping {key}')

                continue

        #READ JSON FILES
        elif key.endswith('.json'):

            s3_object=s3.get_object(
                Bucket=bronze_bucket,
                Key=key
            )

            content=s3_object['Body'].read()

            #try UTF-8 first
            try:
                text= content.decode('utf-8')

            except UnicodeDecodeError:
                text= content.decode(
                    'latin1',
                    errors='replace'
                )

            data= json.loads(text)

            #Youtube category json
            if(
                isinstance(data, dict) and 'items' in data
            ):
                df= pd.json_normalize(data['items'])

            #standard JSON array
            elif isinstance(data, list):
                df = pd.DataFrame(data)

            #other json objects
            else:
                df=pd.json_normalize(data)

            print('JSON  loaded successfully')

        else:
            print(f'Skipping {key}')
            continue
        #----------------------
        #TRANSFORMATION
        #----------------------
        df.columns = df.columns.str.lower()

        #droping duplicates
        df.drop_duplicates(inplace=True)

        #fillna
        df.fillna("unknown", inplace=True)


        #------------------
        #CONVERT TO PARQUET
        #------------------

        parquet_buffer=BytesIO()

        df.to_parquet(parquet_buffer,
                      engine='pyarrow',
                      index=False)

        #create silver path
        silver_key=(key.replace(
            "raw/",
            "cleaned/"
        ).replace(
            ".csv",
            ".parquet"
        ).replace(
            ".json",
            ".parquet"
        ))

        #upload to silver
        s3.put_object(
            Bucket=silver_bucket,
            Key=silver_key,
            Body=parquet_buffer.getvalue()
        )
        print(f'Uploaded {silver_key}')

    except Exception as e:
        print(f'Failed processing {key}')
        print(e)

print('Done')
