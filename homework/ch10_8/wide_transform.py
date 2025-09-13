from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count
import time

spark_conf = {
    'spark.sql.adaptive.enabled': 'false',
    'spark.executor.cores': '2',
    'spark.executor.memory': '2g',
    'spark.executor.instances': '3'
}

spark = SparkSession \
    .builder \
    .appName("wide_transform.py") .getOrCreate() \
    .config(map= spark_conf) \
    .getOrCreate()

print(f'spark application start')

job_skill_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
job_skill_schema = 'job_id LONG, skill_abr STRING'
skill_name_path = 'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'
skill_name_schema = 'skill_abr STRING, skill_name STRING'

job_skills_df = spark.read \
            .option('header','true') \
            .option('multiLine','true') \
            .schema(job_skill_schema) \
            .csv(job_skill_path)

print(f'job_skills_df, skill_name_df load 완료')

skills_name_df = spark.read \
            .option('header','true') \
            .option('multiLine','true') \
            .schema(skill_name_schema) \
            .csv(skill_name_path)
print(f'skills_name_df load 완료')

cnt_per_skills_df = job_skills_df.join(
    other=broadcast(skills_name_df),
    on='skill_abr',
    how='inner'
).select('job_id', 'skill_name') \
    .groupBy('skill_name') \
    .agg(count('job_id').alias('job_count')) \
    .sort('job_count', ascending=False)

print(cnt_per_skills_df.count())
time.sleep(1200)


