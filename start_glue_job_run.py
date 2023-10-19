import argparse
import boto3

def get_glue_client(profile_name, region_name):
    session = boto3.session.Session(profile_name=profile_name, region_name=region_name)
    client = session.client('glue')
    return client

def make_glue_job_args(input_bucket_name, output_bucket_name, log_date_prefix, output_partition_count):
    glue_job_args = {
        '--input_bucket_name': input_bucket_name,
        '--output_bucket_name': output_bucket_name,
        '--log_date_prefix': log_date_prefix,
        '--output_partition_count': output_partition_count,
    }
    return glue_job_args

def main(args):
    client = get_glue_client(args.profile_name, args.region_name)
    glue_args = make_glue_job_args(args.input_bucket_name, args.output_bucket_name, args.log_date_prefix, args.output_partition_count)
    client.start_job_run(JobName=args.glue_job_name, Arguments=glue_args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile-name', type=str, required=True)
    parser.add_argument('--region-name', type=str, required=True)
    parser.add_argument('--input-bucket-name', type=str, required=True)
    parser.add_argument('--output-bucket-name', type=str, required=True)
    parser.add_argument('--log-date-prefix', type=str, required=True)
    parser.add_argument('--glue-job-name', type=str, required=True)
    parser.add_argument('--output-partition-count', type=str, default=48)
    args = parser.parse_args()
    main(args)
