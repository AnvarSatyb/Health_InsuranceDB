Hello, ny name is Anvar Satybaldiev and this is my Case Study for Data Engineering Course in Per Scholas school

Please read carefully 

Follow the following steps in order to this programm working properly
   

This program allows the user to get information about customers and transactions using a program written in Python. 

The data will be extracted from MariaDB, CSV and JSON files, then will be transformed using Kafka, Python, Pyspark, Spark Streaming 
and will be loaded into MongoDB using Python. 

Finally, in MongoDB the data can been analyzed and visualized using different tools.

To start working with this programm please:
1. You need to start your Zookeper
You can open you Command Prompt and type C:\kafka\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

2.Then you also need to start your Kafka Broker
You can open you Command Prompt and type  C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties

You can download and import this zip file inside of your Eclipse programm and run the Entrance page and then you can choose from the options

Functional Requirements

#1 Loading from MariaDB to MongoDB using Spark


1.a - If you would like to see branches data select this option
All the data from CDW_SAPP database branches table from MariaDB will be loaded to MongoDB using Spark

1.b - If you would like to see credit card data select this option
All the data from CDW_SAPP database credit card table from MariaDB will be loaded to MongoDB using Spark

1.c- If you would like to see customers data select this option
All the data from CDW_SAPP database customers table from MariaDB will be loaded to MongoDB using Spark

1.d- Otherwise please press (d), and you will be logged out from the system

After completing this phase all 3 tables can be seen in MongoDB. 


-----------------------------------------------------------------------------------------------------------------

#2 Loading from CSV and JSON files to MongoDB using Spark

2.a - If you would like to see options with Benefits Cost Sharing data select this option
	a.a This option will load its First Part
	a.b This option will load its SecondPart
	a.c This option will load its Third Part
	a.d This option will load its Fourth Part

2.b - If you would like to see Insurance data and load it into MongoDB select this option

2.c - If you would like to see Plan Attributes data and load it into MongoDB select this option

2.d - If you would like to see Network data and load it into MongoDB select this option

2.e - If you would like to see Service Area data and load it into MongoDB select this option

2.f - Otherwise please press g, and you will be returned to the previous menu

After completing this phase all 5 tables can be seen in MongoDB. 


------------------------------------------------------------------------------------------------------------------

#3 Now we can finally analyze our data from MongoDB using various visulization tools like pandas and matplotlib


3.a - If you would like to plot state counts of ServiceAreaName, SourceName, and BusinessYear select this option

3.b - If you would like to plot the counts of sources across the country select this option

3.c - If you would like to see table of the names of the plans with the most customers by state select this option 
(This assignment was cancelled by instructor due to mistakes inside of this task requirements)

3.d - If you would like to plot the number of benefit plans in each state select this option

3.e - If you would like to see the quantity of smoking mothers select this option

3.f - If you would like to plot the rate of smokers for each region select this option

Otherwise please press any button, and you will return to the previous menu

------------------------------------------------------------------------------------------------------------------

END