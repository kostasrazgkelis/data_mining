"""
Author: Konstantinos Razgkelis
Date:   19/11/2021

In order to use this scipt you only need to press the PLAY button

Complexity =  N/k  
(N = the size of the data , k = the numbers of CPUS)
"""



from datetime import date, datetime, timedelta
import traceback
import urllib3
import xmltodict
import pandas as pd
import concurrent.futures
import os
import csv 

#initialize the lists
URLS = ["https://www.wired.com/sitemap.xml","https://www.gq.com/sitemap.xml"]
FINAL_URL = list()

def main(days):

    #We will be using the datetime module in order to get the date
    today = date.today()
    yesterday = today - timedelta(days = days)

    #We will be using multithreading in order to utilize all of the resources and enhance the computational speed 
    with concurrent.futures.ThreadPoolExecutor (max_workers=os.cpu_count()) as executor:

        result = {executor.submit(getxml, url): url for url in URLS}
        for future in concurrent.futures.as_completed(result):

            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % ( exc))
            else:
                
                #We are using the required fields and conditions in order to filter out the incorrect data 
                #while saving the desired into a list
                for x in data['sitemapindex']['sitemap']:

                    if str(x['lastmod']) >= str(yesterday):                
                        FINAL_URL.append (str(x['loc']))

        #We split again all the URLS resulted from the last filtering in otder to multiprocess the data  
        result = {executor.submit(getxml, url): url for url in FINAL_URL}  

        FINAL_URL.clear()
        for future in concurrent.futures.as_completed(result):

            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % ( exc))
            else:                 
  
                for row in data['urlset']['url']:
                    
                    #We are using the required fields and conditions in order to filter out the incorrect data 
                    #while saving the desired into a list 
                    after_format = datetime.fromisoformat( str(row['lastmod'])[:-1] )
                    after_format.strftime('%Y-%m-%d ')

                    #We are using the required fields and conditions in order to filter out the incorrect data  
                    if after_format.year == yesterday.year and after_format.month == yesterday.month and after_format.day == yesterday.day and '/story/' in row['loc']:
                    
                        #We finally save the final data in our list
                        FINAL_URL.append(row['loc'])


    #Before we save the data to a text file we need to clear out the duplicates
    #For that purpose we will convert our list into a set. (set has only unique values)
    
    with open(f'{yesterday}_URLs.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)

        for row in set(FINAL_URL):
            row = row.split(',')
            spamwriter.writerow(row)

    FINAL_URL.clear()



                   
                      
def getxml(url):
    #We create the a poolmanager object that handles our http connections
    http = urllib3.PoolManager()

    #We get the response from the request which has the data from the URL
    response = http.request('GET', url)

    try:
        #We need to format the data so that we can manipulate them for our goals
        data = xmltodict.parse(response.data)
    except ConnectionError as e:
        print(f"Connection Failed {e}")
    except:
        print("Failed to parse xml from response (%s)" % traceback.format_exc())
    else:
        return data


if __name__ == "__main__":
    
    #by running the app it will start generating data from the day before today
    #if you want to see information from earlier days you need to change the
    #variable number_of_past_days depending on how many days you want to see
    #
    #example: if you want to see all data from 3 days ago up until last day 
    #number_of_past_days must be equal to 3

    number_of_past_days = 100
    for x in range(1, number_of_past_days):
        main(x)
        