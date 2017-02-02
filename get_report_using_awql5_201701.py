# encoding=utf8    

import sys
import multiprocessing
import time
import os
import csv
#import aw_oauth

from googleads import adwords
from googleads import oauth2
from Queue import Queue

reload(sys)  
sys.setdefaultencoding('utf8')

__author__ = 'Cleiver Moreno'

# AdWords API information.
# AdWords API information.

import datetime
from datetime import timedelta

yesterday = datetime.date.today() + timedelta(days = -1)
startdate = datetime.date.today() + timedelta(days = -365)
datestring = 'DURING' + startdate.strftime('%Y%m%d') + ', ' + yesterday.strftime('%Y%m%d') 

#print datestring 



_FILE_SUFFIX = "ADGROUP_PERFORMANCE_REPORT.csv"

'''
report = {
      'reportName': 'DOES RW',
      'dateRangemin': '20140901',
      'dateRangemax': '20150630',
	  'dateRangeType': 'CUSTOM_DATE',
      'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
      'downloadFormat': 'CSV',
      'selector': {
          'fields': ['AccountDescriptiveName','CampaignName','CampaignId','Week','Impressions',
                     'Cost','Conversions','ConversionsManyPerClick','ConversionValue','AveragePosition','Clicks','SearchImpressionShare'],
      }
  }
'''

_REPORT_AWQL = ('SELECT Week, AccountDescriptiveName, CampaignName, CampaignId, CampaignStatus, AdGroupName, AdGroupId ,AdGroupStatus, '
                'Labels, LabelIds, Device, Impressions, Cost, Conversions, ConvertedClicks,'
                'ConversionValue, AveragePosition, Clicks '
				'FROM ADGROUP_PERFORMANCE_REPORT '
				'DURING LAST_30_DAYS')


def download_report(client,client_customer_id,client_folder):

  report_downloader = client.GetReportDownloader(version='v201605')
  # Create report definition.
  report_query = _REPORT_AWQL
  file_path = str('C:/Users/janitua/Documents/PYTHON/BQ DATA/AdWords Data')  + '/' + str('lists/') +str(client_folder) + "/"+ str(client_customer_id) + _FILE_SUFFIX

  if not os.path.exists(os.path.dirname(file_path)):
    os.makedirs(os.path.dirname(file_path))
  
  # Retrieve the report stream and print it out
  with open(file_path, 'w') as output_file:
    report_downloader.DownloadReportWithAwql(report_query, 'CSV', output_file)
	

def main(account):
  #try:
    client_customer_id  =account[0]
    print client_customer_id
    client_folder       =account[2]
    print client_folder
    adwords_client = adwords.AdWordsClient.LoadFromStorage()    
    adwords_client.SetClientCustomerId(client_customer_id)
    download_report(adwords_client,client_customer_id,client_folder)

  #except:
   # pass
  
if __name__ == '__main__':
	with open('google_accounts.txt', 'rb') as sub:
		reader = csv.reader(sub,delimiter='\t')
		for line in  reader:
			account = line
			#print account
			#print "******"
			results  = main(account)