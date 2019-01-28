# 				Cloud Computing
# 				Project 2
# 				README.txt
# 				12/06/2018
#

Data for this project is taken from https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95

Task#1: Clean the input data1. Keep following columns in the cleaned data1.1. Date1.2. Borough1.3. Zip1.4. Number Of Persons Injured1.5. Number Of Persons Killed1.6. Number Of Pedestrians Injured1.7. Number Of Pedestrians Killed1.8. Number Of Cyclist Injured1.9. Number Of Cyclist Killed1.10. Number Of Motorist Injured1.11. Number Of Motorist Killed1.12. Vehicle Type Code 12. Remove any row if value of any of the above mentioned column is missing (0 isallowed).

Task#2: Process the cleaned data for following information.1. Date on which maximum number of accidents took place.2. Borough with maximum count of accident fatality.3. Zip with maximum count of accident fatality.4. Which vehicle type is involved in maximum accidents.5. Year in which maximum Number Of Persons and Pedestrians Injured.6. Year in which maximum Number Of Persons and Pedestrians Killed.7. Year in which maximum Number Of Cyclist Injured and Killed (combined).8. Year in which maximum Number Of Motorist Injured and Killed (combined).

Task#3: Write code to process output file from Task#2 and generate final output filewith all the required numbers from Task#2Submission must include:1. Code used for cleaning the input data (either Map-Reduce or Spark).2. Map-Reduce code along with jar file (if you are using java). Maximum number offiles allowed is 3 (code must be properly commented).3. Spark code (maximum number of files 3) (code must be properly commented).4. Code used for Task#3.5. Text file with all 8 result from Task#3.6. Readme file with description of functionality for each of the above files.

The project is completed in 5 stages.

Stage1: #Task1 is performed using Spark.

It is to clean the input data. Cleaning data is done by selecting the 12 columns.
The column names are as follows:
1. Date2. Borough3. Zip4. Number Of Persons Injured5. Number Of Persons Killed6. Number Of Pedestrians Injured7. Number Of Pedestrians Killed8. Number Of Cyclist Injured9. Number Of Cyclist Killed10. Number Of Motorist Injured11. Number Of Motorist Killed12. Vehicle Type Code 1

If any row that does not have a value in any of the above mentioned columns then that row is deleted.

The data is in /user/data/nypd/ on hadoop cluster.
The code is written in Spark using Python programming language.

The file name is NYPD_Cleaning_Data.py

Command to run the Cleaning data file:
spark-submit NYPD_Cleaning_Data.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt

Output Location of Cleaned data: /user/vuyyurra/CleanDataBySpark

Command to check number of records generated after Cleaning the data:
hdfs dfs -cat /user/vuyyurra/CleanDataBySpark/* |wc

The above command gives three values. The first value indicates number of lines, which is nothing but number of records.

There are 967234 records.


Stage2: #Task2

File name is NYPD_Data_Task2.py

It performs the following queries:
1. Date on which maximum number of accidents took place.2. Borough with maximum count of accident fatality3. Zip with maximum count of accident fatality4. Which vehicle type is involved in maximum accidents5. Year in which maximum Number Of Persons and Pedestrians Injured6. Year in which maximum Number Of Persons and Pedestrians Killed7. Year in which maximum Number Of Cyclist Injured and Killed (combined)8. Year in which maximum Number Of Motorist Injured and Killed (combined)

Command to run this file:
spark-submit NYPD_Data_Task2.py /user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt

Command to check the list of output files generated:
hdfs dfs -ls /user/vuyyurra/task2/output2
There will 9 files.

Output Location: /user/vuyyurra/task2/output2

Command to check the output of Task 2:
hdfs dfs -cat /user/vuyyurra/task2/output2/part*

The output generated is as follows, for the above mentioned 8 queries.
01/21/2014,842
BROOKLYN,663
11236,54
PASSENGER VEHICLE,502983
2013,52791
2013,345
2015,3876
2013,27622

Each line represents each output of the queries respectively.


Stage3: #Task3 Python code:

File name is NYPD_Task3.py

A directory is created to store the final result.
Command: mkdir FinalResults

FinalResults folder location is /home/vuyyurra/data/FinalResults

Command to copy results of Task2 to FinalResults folder:
hdfs dfs -copyToLocal /user/vuyyurra/task2/output2/* FinalResults/

Command to run Task3 output:
python NYPD_Task3.py

The output generated is as follows:
['01/21/2014', '842']
['BROOKLYN', '663']
['11236', '54']
['PASSENGER VEHICLE', '502983']
['2013', '52791']
['2013', '345']
['2015', '3876']
['2013', '27622']


Stage4: Generating key, value pairs using MapReduce:

FileName is NYPD_task2_MapReduce.java

Name of jar file is MapReduce1.jar

MapReduce1.jar to hadoop is copied to hadoop on /home/vuyyurra/data

Command to run jar file:
yarn jar data/MapReduce1.jar vuyyuru.map.reduce.NYPD_task2_MapReduce /user/vuyyurra/CleanDataBySpark /user/vuyyurra/output99

Command to copy the output to local:
hdfs dfs -copyToLocal /user/vuyyurra/output99/part* data/

Command to check the number of key, value pairs generated:
hdfs dfs -cat /user/vuyyurra/output99/* | wc

The above command gives three values. The first value indicates number of key, value pairs generated, here 2956 key, value pairs are generated.


Stage5: #Task3 using Java code:

File name is Task3.java

File location is /home/vuyyurra/data

Change directory to the above location.

Command to compile the code:
javac Task3.java

Command to see the output:
java Task3

The output generated is as follows:
1. Date on which maximum number of accidents took place:  01/21/2014  842
2. Borough with maximum count of accident fatality:  BROOKLYN 663
3. Zip with maximum count of accident fatality:  11236 54
4. Which vehicle type is involved in maximum accidents:  PASSENGER VEHICLE 502983
5. Year in which maximum Number Of Persons and Pedestrians Injured:  2013 52791
6. Year in which maximum Number Of Persons and Pedestrians Killed:  2013 345
7. Year in which maximum Number Of Cyclist Injured and Killed:  2015 3876
8. Year in which maximum Number Of Motorist Injured and Killed:  2013 27622