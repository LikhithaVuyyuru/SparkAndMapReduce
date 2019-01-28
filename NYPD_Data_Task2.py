#importing packages and libraries
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys

#Defining App Name
APP_NAME = "NYPD_Task_2"

#main functionality
def main(spark,file2):
    #Cleaning Data
    new2_data=spark.read.csv(file2,header=True);
    req2_data=new2_data.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')
    final2_data=req2_data.filter(col("#DATE").isNotNull() & col("BOROUGH").isNotNull() & col("ZIP CODE").isNotNull() & col("NUMBER OF PERSONS INJURED").isNotNull() & col('NUMBER OF PERSONS KILLED').isNotNull() & col("NUMBER OF PEDESTRIANS INJURED").isNotNull() & col("NUMBER OF PEDESTRIANS KILLED").isNotNull() & col("NUMBER OF CYCLIST INJURED").isNotNull() & col("NUMBER OF CYCLIST KILLED").isNotNull() & col("NUMBER OF MOTORIST INJURED").isNotNull() & col("NUMBER OF MOTORIST KILLED").isNotNull() & col("VEHICLE TYPE CODE 1").isNotNull())
    #Storing the output in a table
    final2_data.registerTempTable("outputs")
    #Date on which maximum number of accidents took place
    t_1=spark.sql("select `#date`,count(*) as total from outputs group by `#date` order by total desc  limit 1")
    all_outputs=t_1
    #Borough with maximum count of accident fatality
    t_2=spark.sql("select borough,sum(int(`NUMBER OF PERSONS KILLED`+`NUMBER OF CYCLIST KILLED`\
                +`NUMBER OF PEDESTRIANS KILLED`+`NUMBER OF MOTORIST KILLED`)) \
                as total from outputs group by borough order by total desc  limit 1")
    all_outputs = all_outputs.union(t_2)
    #Zip with maximum count of accident fatality
    t_3=spark.sql("select `ZIP CODE`,sum(int(`NUMBER OF PERSONS KILLED`+`NUMBER OF CYCLIST KILLED`\
                +`NUMBER OF PEDESTRIANS KILLED`+`NUMBER OF MOTORIST KILLED`))\
                as total from outputs group by `ZIP CODE` order by total desc limit 1")    
    all_outputs = all_outputs.union(t_3)
    #Which vehicle type is involved in maximum accidents
    t_4=spark.sql("select `VEHICLE TYPE CODE 1`, count(*) as total from outputs group by `VEHICLE TYPE CODE 1` order by total desc  limit 1")
    all_outputs = all_outputs.union(t_4)
    #Year in which maximum Number Of Persons and Pedestrians Injured
    t_5=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF PERSONS INJURED`+\
                 `NUMBER OF PEDESTRIANS INJURED`)) as total from outputs group by `Year` order by total desc limit 1")
    all_outputs = all_outputs.union(t_5)
    #Year in which maximum Number Of Persons and Pedestrians Killed
    t_6=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF PERSONS KILLED`+\
                `NUMBER OF PEDESTRIANS KILLED`)) as total from outputs group by `Year` order by total desc limit 1")
    all_outputs = all_outputs.union(t_6)
    #Year in which maximum Number Of Cyclist Injured and Killed (combined)
    t_7=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF CYCLIST INJURED`+\
                `NUMBER OF CYCLIST KILLED`)) as total from outputs group by `Year` order by total desc limit 1")
    all_outputs = all_outputs.union(t_7)
    #Year in which maximum Number Of Motorist Injured and Killed (combined)
    t_8=spark.sql("select year(to_date(cast(unix_timestamp(`#date`, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(`NUMBER OF MOTORIST INJURED`+\
                `NUMBER OF MOTORIST KILLED`)) as Total from outputs group by `Year` order by total desc limit 1")
    all_outputs = all_outputs.union(t_8)
    #All outputs are written to a folder
    all_outputs.write.format("csv").save("/user/vuyyurra/task2/output2")

if __name__ == "__main__":
   #Spark Configuration
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   file2=sys.argv[1]
   #Calling main function
   main(spark,file2)
