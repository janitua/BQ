#!/usr/bin/python
# encoding: utf-8
import ntpath
import itertools as IT
import numpy as np
import glob  
import csv
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
from time import gmtime, strftime
from google.cloud import storage
storage_client = storage.Client()
time = strftime("%Y%m%d", gmtime())

regions = ["APAC","EMEA","GLOBAL","LATAM","NA"]




def Join_files():
	for region in regions:
		folder = 'C:/Users/janitua/Documents/PYTHON/BQ DATA/RLSA_report/lists/' + str(region) +'//*csv'
		files=glob.glob(folder)   
		headers1= ['Week', 'Account', 'Campaign', 'Campaign_ID', 'Campaign_State', 'AdGroup', 'AdGroup_ID', 'AdGroup_State', 'Criterion_ID', 'Audience', 'Audience_State', 'Device', 'Bid_Adj', 'Impressions', 'Cost', 'Conversions', 'Converted_Clicks', 'Total_Conv_Value', 'Avg_Position', 'Clicks']
		headers2= ['Week', 'Account', 'Campaign', 'Campaign_ID', 'Campaign_State', 'AdGroup', 'AdGroup_ID', 'AdGroup_State', 'Criterion_ID', 'Audience', 'Audience_State', 'Device', 'Bid_Adj', 'Impressions', 'Cost', 'Conversions', 'Converted_Clicks', 'Total_Conv_Value', 'Avg_Position', 'Clicks', 'POSu', 'POSa', 'Traffic_type', 'PropertyID', 'DestinationID']
		print region, len(files)
		out = pd.DataFrame(columns=headers2)
		if len(files)>0: #if the folder is empty
			for file in files:
				frame=pd.read_csv(os.path.join(file), skiprows=range(0, 1),header=0, names=headers1,thousands=',',low_memory=False)
				frame=frame[(frame.Week != 'Total') & (frame.Week != 'Week')]
				if len(frame.index)> 0:
					frame['Cost'] = frame['Cost']/1000000
					frame['Campaign_Info']=frame['Campaign'].str[:11]
					frame['Traffic_type'], frame['POSu'], frame['POSa']= zip(*frame['Campaign_Info'].apply(lambda x: x.split(':')))
					frame['Audience']= frame['Audience'].str.extract('(\d+)')
					frame['PropertyID'] = np.where(frame['Traffic_type']=='P',  frame['AdGroup'].str.extract('(\d+)'), '')
					frame['DestinationID'] = np.where(frame['Traffic_type']!='P',  frame['AdGroup'].str.extract('(\d+)'), '')
					out = pd.concat([out, frame],  ignore_index=True)
					
				os.remove(file)
			out.to_csv('C:/Users/janitua/Documents/PYTHON/BQ DATA/RLSA_report/output/' + str(region) + '/list_' + str(region) + '.csv',columns=headers2,header=False, index=False) 

def upload_files():
	bucket = storage_client.get_bucket('rlsa')
	for region in regions:
	#blob.upload_from_string('New contents!')
		blob2 = bucket.blob(str(region)+ '/list_' + str(region) + '.csv')
		blob2.upload_from_filename(filename='C:/Users/janitua/Documents/PYTHON/BQ DATA/RLSA_report/output/' + str(region) + '/list_' + str(region) + '.csv')
		print 'File list_' + str(region) + '.csv Uploaded'
	'''file = 'list_APAC.csv'
    bucket = storage_client.get_bucket('rlsa')
    blob2 = bucket.blob('C:/Users/cmoreno/Documents/AdWords Data/lists/output/APAC/list_APAC.csv')
	
    blob2.upload_from_filename(filename='list_APAC')'''
def main():
	Join_files()
	upload_files()
	
	
if __name__ == '__main__':
	main()
	


