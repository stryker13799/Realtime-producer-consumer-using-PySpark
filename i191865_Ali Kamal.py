#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import os
import pandas as pd
import time
import glob
from IPython.display import clear_output


# In[2]:


spark = SparkSession     .builder     .appName("AliKamal i19-1865 Big Data Analytics Bonus Assignment")     .getOrCreate()


# In[3]:


spark


# In[4]:


def check_if_exists(item,arr):
    if item in arr:
        return True
    return False


# In[5]:


def generator_script():
    global low_limit,upper_limit,flag,df
    count=0
    t0=time.time()
    df2=df[low_limit:upper_limit]
    if(df2.empty):
        flag=1
        return -1
    low_limit+=10
    upper_limit+=10
    for index, row in df2.iterrows():
        if(time.time()-t0<=10):
            count+=1
            tempdf=pd.DataFrame({'id': [row[0]],'name': [row[1]],'stock':[row[4]],'reviews':[row[5]],'rating':[row[7]]})
            tempdf.to_csv(path+'/'+str(count)+".csv")
        else:
            files = glob.glob(path+'/*') #REMOVING ALL FILES IN FILES FOLDER PATH AS WE HAVE RUN OUT OF TIME
            for f in files:
                os.remove(f)
            return -1


# In[6]:


def consumer_script():
    global low_limit,prev_records
    curr_items=[]        
    t0=time.time()
    clear_output(wait=True) #Refreshing dashboard
    print("Loading new batch now...\n")
    for i in [1,2,3,4,5,6,7,8,9,10]:
        if(time.time()-t0<=20):
            df3 = spark.read.format("csv").option("header", "true").load(path+"/"+str(i)+".csv")
            df3=df3.withColumn('stock', regexp_replace('stock', 'new', ''))
            df3=df3.withColumn('stock', regexp_replace('stock', 'used', ''))
            df3=df3.withColumn('rating', regexp_replace('rating', ' out of 5 stars', ''))
            try:
                if(float(df3.collect()[0][5])>=4.2 and float(df3.collect()[0][4])>=5.0 and float(df3.collect()[0][3])<5.0): #checking conditions for stock request
                    if (low_limit==10): #First batch
                        prev_record.append(df3.collect()[0][1]) #adding product id to keep record of previous batch
                        print(df3.collect()[0][2]) 
                    else:
                        curr_items.append(df3.collect()[0][1])
                        if(check_if_exists(df3.collect()[0][1],prev_records)==False): #check if item in previous batch
                            print(df3.collect()[0][2])  
            except:
                None
        else:
            break
    if(low_limit>10): #Not first batch
        prev_records = curr_items[:] 
    if(time.time()-t0<=20):
        print("\nNO MORE RELEVANT RECORDS TO SHOW IN THIS BATCH")
    del_flag=0
    while(time.time()-t0<=20):
        if(del_flag==0 and time.time()-t0>=15): #DELETING FILES AT 15TH SECOND
            del_flag=1
            files = glob.glob(path+'/*')
            for f in files:
                os.remove(f)
        1+2#do nothing
    if(del_flag==0): #IF 20 SECONDS HAVE ALREADY PASSED AND CONSUMER COULDN'T FINISH IN TIME, SO DELETING REMAINING FILES
        files = glob.glob(path+'/*')
        for f in files:
            os.remove(f)


# In[11]:


parent_dir="C:/Users/Ali/Desktop/BDA Bonus Assignment/" #PATH IS RELATIVE TO YOUR SYSTEM
directory="File"
path=os.path.join(parent_dir, directory)
try: 
    os.mkdir(path) 
except OSError as error: 
    print(error)  


# In[8]:


df = pd.read_csv("amazon_co-ecommerce_sample.csv") #SAMPLE DATASET IS IN SAME FOLDER AS IPYNB FILE


# In[41]:


flag=0
low_limit=0
upper_limit=10
prev_record=[]
    

while(flag==0): #RUNNING BOTH GENERATOR AND CONSUMER SCRIPTS
    if(generator_script()==-1):
        continue
    consumer_script()


# In[10]:


spark.stop()

