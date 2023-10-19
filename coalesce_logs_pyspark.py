import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import re
import logging
from datetime import datetime
import boto3
from pyspark.sql.functions import regexp_extract, col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'input_bucket_name',
                                     'output_bucket_name',
                                     'log_date_prefix',
                                     'output_partition_count'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = glueContext.spark_session

bucket_name = args['input_bucket_name']
date_prefix = args['log_date_prefix']

log_regex_pattern = r'^\S+\s(\S+)\s\[(.+)\]\s(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s(\S+)\s\S+\s\S+\s(\S+)\s\"((GET|POST|HEAD|DELETE|OPTIONS|TRACE|PUT)\s(\S+)\sHTTP/(\S+))\"\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s\S+\s\S+\s\"(\S+)\"\s\"(.+)\"'

def list_objects_with_prefix(bucket_name, prefix):
    s3 = boto3.client('s3')
    objects = []
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
    
    while True:
        response = s3.list_objects_v2(**kwargs)
        if 'Contents' in response:
            objects.extend(response['Contents'])

        try:
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        except KeyError:
            break

    return [obj['Key'] for obj in objects]


prefixed_objects = list_objects_with_prefix(bucket_name, date_prefix)
prefixed_paths = (f's3://{bucket_name}/{filename}' for filename in prefixed_objects)
raw_log_df = spark.read.text(prefixed_paths)

df = raw_log_df.select(
    regexp_extract('value', log_regex_pattern, 1).alias('bucket'),
    regexp_extract('value', log_regex_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_regex_pattern, 3).alias('ip_address'),
    regexp_extract('value', log_regex_pattern, 4).alias('requester'),
    regexp_extract('value', log_regex_pattern, 5).alias('s3_location'),
    regexp_extract('value', log_regex_pattern, 6).alias('request'),
    regexp_extract('value', log_regex_pattern, 7).alias('request_method'),
    regexp_extract('value', log_regex_pattern, 8).alias('request_url'),
    regexp_extract('value', log_regex_pattern, 9).alias('http_version'),
    regexp_extract('value', log_regex_pattern, 10).alias('http_status'),
    regexp_extract('value', log_regex_pattern, 11).alias('error_code'),
    regexp_extract('value', log_regex_pattern, 12).alias('bytes_sent'),
    regexp_extract('value', log_regex_pattern, 13).alias('object_size'),
    regexp_extract('value', log_regex_pattern, 14).alias('referrer'),
    regexp_extract('value', log_regex_pattern, 15).alias('user_agent')
)

df_gets_only = df.filter(df.request_method == 'GET')
uuid_regex_pattern=r'^[0-9]+\/[0-9]+\/[0-9]+\/(\S+)\/'
df_gets_only = df_gets_only.withColumn('uuid', regexp_extract(col('s3_location'), uuid_regex_pattern, 1))
df_gets_only = df_gets_only.coalesce(int(args['output_partition_count']))
df_gets_only.write.parquet(f"s3://{args['output_bucket_name']}/{args['log_date_prefix']}")
job.commit()
