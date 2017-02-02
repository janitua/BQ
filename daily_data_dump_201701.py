#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loads data into BigQuery from an object in Google Cloud Storage.
For more information, see the README.md under /bigquery.
Example invocation:
    $ python load_data_from_gcs.py example_dataset example_table \
        gs://example-bucket/example-data.csv
The dataset and table should already exist.
"""

import argparse
import time
import uuid

from google.cloud import bigquery
regions = ["APAC","EMEA","GLOBAL","LATAM","NA"]

def load_data_from_gcs(dataset_name, table_name, source):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    job_name = str(uuid.uuid4())

    job = bigquery_client.load_table_from_storage(
        job_name, table, source)

    job.begin()

    wait_for_job(job)

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)
def create_table(dataset_name, table_name, project):
    """Creates a simple table in the given dataset.
    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)

    if not dataset.exists():
        print('Dataset {} does not exist.'.format(dataset_name))
        return

    table = dataset.table(table_name)
    # Set the table schema
    table.schema = (
		bigquery.SchemaField('Week','STRING'),
        bigquery.SchemaField('Account','STRING'),
        bigquery.SchemaField('Campaign','STRING'),
        bigquery.SchemaField('Campaign_ID','STRING'),
        bigquery.SchemaField('Campaign_State','STRING'),
        bigquery.SchemaField('AdGroup','STRING'),
        bigquery.SchemaField('AdGroup_ID','STRING'),
        bigquery.SchemaField('AdGroup_State','STRING'),
        bigquery.SchemaField('Impressions','FLOAT'),
        bigquery.SchemaField('Cost','FLOAT'),
        bigquery.SchemaField('Conversions','FLOAT'),
        bigquery.SchemaField('Converted_Clicks','FLOAT'),
        bigquery.SchemaField('Total_Conv_Value','FLOAT'),
        bigquery.SchemaField('Avg_Position','FLOAT'),
        bigquery.SchemaField('Clicks','FLOAT'),
        bigquery.SchemaField('POSu','STRING'),
        bigquery.SchemaField('POSa','STRING'),
        bigquery.SchemaField('Traffic_type','STRING'),
        bigquery.SchemaField('PropertyID','STRING'),
        bigquery.SchemaField('DestinationID','STRING'),
		bigquery.SchemaField('LPS_Flag','STRING'),
    )

    table.create()

    print('Created table {} in dataset {}.'.format(table_name, dataset_name))

def delete_table(dataset_name, table_name, project):
    """Deletes a table in a given dataset.
    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    table.delete()

    print('Table {}:{} deleted.'.format(dataset_name, table_name))


if __name__ == '__main__':
    
    project='peaceful-leaf-665'
    dataset_name='AdWords_datasets'
    for region in regions:
		table_name=region+"_last_30_days"
		source='gs://adwords_data/'+region+'/list_'+region+'.csv'
		delete_table(dataset_name, table_name, project)
		create_table(dataset_name, table_name, project)
		load_data_from_gcs(dataset_name, table_name, source)