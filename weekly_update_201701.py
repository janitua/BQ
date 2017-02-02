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

"""Command-line application to perform asynchronous queries in BigQuery.
For more information, see the README.md under /bigquery.
Example invocation:
    $ python async_query.py \\
          'SELECT corpus FROM `publicdata.samples.shakespeare` GROUP BY corpus'
"""
import datetime
import argparse
import time
import uuid

from google.cloud import bigquery
from google.cloud.bigquery import table
#regions = ["APAC","EMEA","GLOBAL","LATAM","NA"]
regions = ["APAC"]
date1 = datetime.date.fromordinal(datetime.date.today().toordinal()-28)
Last_28_days = "'"+str(date1)+"'"





def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def async_query(query, project_id,dataset_name, table_name):
	print project_id
	print dataset_name
	print table_name
	client = bigquery.Client(project=project_id)
	dataset = client.dataset(dataset_name)
	table = dataset.table(name=table_name)
	query_job = client.run_async_query(str(uuid.uuid4()), query)
	query_job.use_legacy_sql = True
	query_job.allow_large_results = True
	query_job.destination = table
	query_job.create_disposition = 'CREATE_IF_NEEDED'
	query_job.write_disposition = 'WRITE_TRUNCATE'
	query_job.begin()
	#print query_job
	wait_for_job(query_job)

    # Drain the query results by requesting a page at a time.
	query_results = query_job.results()
	page_token = None

    #while True:
    #    rows, total_rows, page_token = query_results.fetch_data(
     #       max_results=10,
     #       page_token=page_token)

    #    for row in rows:
    #        print(row)

    #    if not page_token:
    #        break

def main():
  project_id = 'peaceful-leaf-665'	
  dataset_name='RLSA'  
  client = bigquery.Client(project=project_id)           
  for region in regions: 

    query_0 = "SELECT a.Week as Week,  a.Account as Account,  a.Campaign as Campaign,  a.Campaign_ID as Campaign_ID,  a.Campaign_State as Campaign_State,\
                a.AdGroup as AdGroup,  a.AdGroup_ID as AdGroup_ID,  a.AdGroup_State as AdGroup_State,  a.Criterion_ID as Criterion_ID,\
                a.Audience AS Audience,  a.Audience_State AS Audience_State,  a.Device AS Device,   a.Bid_Adj AS Bid_Adj,  a.Impressions AS Impressions,\
                a.Cost as Cost,  a.Conversions as Conversions,  a.Converted_Clicks as Converted_Clicks,  a.Total_Conv_Value as Total_Conv_Value,\
                a.Avg_Position as Avg_Position,  a.Clicks as Clicks,  a.POSu as POSu,  a.POSa as POSa,  a.Traffic_type as Traffic_Type,\
                a.PropertyID as PropertyID,  a.DestinationID as DestinationID,  a.Audience_Name as Audience_Name,  a.Estimated_Audience_Size as Estimated_Audience_Size,\
                a.POSA_Super_Region as POSA_Super_Region,  a.POSA_Country as POSA_Country \
                FROM   [RLSA."+region+"] as a"
  
    query_1 = "SELECT a.Week as Week,  a.Account as Account,  a.Campaign as Campaign,  a.Campaign_ID as Campaign_ID,  a.Campaign_State as Campaign_State,\
                a.AdGroup as AdGroup,  a.AdGroup_ID as AdGroup_ID,  a.AdGroup_State as AdGroup_State,  a.Criterion_ID as Criterion_ID,\
                a.Audience AS Audience,  a.Audience_State AS Audience_State,  a.Device AS Device,   a.Bid_Adj AS Bid_Adj,  INTEGER(a.Impressions) AS Impressions,\
                a.Cost as Cost,  INTEGER(a.Conversions) as Conversions,  INTEGER(a.Converted_Clicks) as Converted_Clicks,  a.Total_Conv_Value as Total_Conv_Value,\
                a.Avg_Position as Avg_Position,  INTEGER(a.Clicks) as Clicks,  a.POSu as POSu,  a.POSa as POSa,  a.Traffic_type as Traffic_Type,\
                a.PropertyID as PropertyID,  a.DestinationID as DestinationID,  b.Name as Audience_Name,  b.Size as Estimated_Audience_Size,\
                c.POSA_Super_Region as POSA_Super_Region,  c.POSa as POSA_Country \
                FROM  [RLSA."+region+"_last_30_days] as a \
                LEFT JOIN EACH [RLSA.list_info] as b ON  b.ID = a.Audience \
                LEFT JOIN EACH [hermione.all_sites] as c ON  c.Feeder = a.POSa where a.Week>="+Last_28_days
    
    query_2   = "SELECT a.Week as Week,  a.Account as Account,  a.Campaign as Campaign,  a.Campaign_ID as Campaign_ID,  a.Campaign_State as Campaign_State,\
                a.AdGroup as AdGroup,  a.AdGroup_ID as AdGroup_ID,  a.AdGroup_State as AdGroup_State,  a.Criterion_ID as Criterion_ID,\
                a.Audience AS Audience,  a.Audience_State AS Audience_State,  a.Device AS Device,   a.Bid_Adj AS Bid_Adj,  a.Impressions AS Impressions,\
                a.Cost as Cost,  a.Conversions as Conversions,  a.Converted_Clicks as Converted_Clicks,  a.Total_Conv_Value as Total_Conv_Value,\
                a.Avg_Position as Avg_Position,  a.Clicks as Clicks,  a.POSu as POSu,  a.POSa as POSa,  a.Traffic_type as Traffic_Type,\
                a.PropertyID as PropertyID,  a.DestinationID as DestinationID,  a.Audience_Name as Audience_Name,  a.Estimated_Audience_Size as Estimated_Audience_Size,\
                a.POSA_Super_Region as POSA_Super_Region,  a.POSA_Country as POSA_Country \
                FROM   [RLSA."+region+"] as a where a.Week<"+Last_28_days

    query_3   = "SELECT Week,  Account,  Campaign,  Campaign_ID,  Campaign_State,\
                AdGroup,  AdGroup_ID,  AdGroup_State, Criterion_ID,\
                Audience, Audience_State, Device, Bid_Adj, Impressions, Cost, Conversions, Converted_Clicks,\
                Total_Conv_Value,\
                Avg_Position, Clicks, POSu, POSa, Traffic_Type,\
                PropertyID, DestinationID, Audience_Name, Estimated_Audience_Size,\
                POSA_Super_Region, POSA_Country \
                FROM   [RLSA."+region+"],[RLSA."+region+"_last_30_days]"
    
    
    print region, "running query0"
    async_query(query_0,project_id,dataset_name, region+"_bkp")
    print region, "running query1"               
    async_query(query_1,project_id,dataset_name, region+"_last_30_days")
    print region, "running query2"               
    async_query(query_2,project_id,dataset_name, region)
    print region, "running query3"               
    async_query(query_3,project_id,dataset_name, region)
    
    
    '''print region, "running query1"
    createFile(bigquery_service, query_1,PROJECT_NUMBER, 'RLSA',region+"_last_30_days")
    print region, "running query2"
    createFile(bigquery_service, query_2,PROJECT_NUMBER, 'RLSA',region)
    print region, "running query3"
    createFile(bigquery_service, query_3,PROJECT_NUMBER, 'RLSA',region)'''

if __name__ == '__main__':
    main()  
    