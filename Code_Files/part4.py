from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StructType, StringType,DateType,FloatType,Row
import pandas as pd
import warnings
import phonenumbers
from datetime import datetime
from geopy.geocoders import Nominatim
from pyspark.sql.types import *
from pyspark.sql.functions import *


warnings.filterwarnings("ignore")

master = 'local'
appName = 'PySpark_Data format Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
sqlContext = SQLContext(sc)
ss = SparkSession(sc)

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

df = pd.read_excel("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/Sample_Excel_4.xlsx")

print(df.head(5))

schema = StructType([StructField('No', StringType(), True),
                     StructField('No2', StringType(), True),
                     StructField('No1', StringType(), True),
                    StructField('Unnamed', StringType(), True),
                    StructField('ID', StringType(),True),
                    StructField('CreationDate', StringType(),True),
                    StructField('DisplayName', StringType(),True),
                    StructField('WebsiteURL', StringType(), True),
                    StructField('Location', StringType(), True),
                    StructField('Views', StringType(), True),
                    StructField('ProfileImageURL', StringType(),True),
                    StructField('About_me', StringType(), True),
                     StructField('Birthdate', StringType(), True),
                     StructField('PhoneNumber', StringType(), True)])


df = ss.createDataFrame(df, schema=schema)
df= df.drop('No','No1','No2','Unnamed','WebsiteURL','Views','ProfileImageURL','CreationDate')


#Display name Validation

def validate_display_name(name):
    import re
    regexp = re.compile(r'[^\s.\w]')
    if regexp.search(name):
        return None
    else:
        return name


# Phone Validation

def validate_phone_number(PhoneNumber,Location):
    try:
        if phonenumbers.is_valid_number(phonenumbers.parse(PhoneNumber, region=Location)):
            return PhoneNumber
        else:
            return None
    except phonenumbers.phonenumberutil.NumberParseException:
        return None


# Birthdate Validation

def birthdate(date):
    format = "%Y-%m-%d"
    try:
        datem = datetime.strptime(date, format)
        if bool(datetime.strptime(date, format))==False:
            return None
        elif datem.year<1970 or datem.year>2004:
            return None
        else:
            return date
    except:
        return None


# Location validation

def locFinder(l):
    try:
        geolocator = Nominatim(user_agent="sample app")
        address=[]
        data = geolocator.geocode(l)
        Lat = data.point.latitude
        Long = data.point.longitude
        locn = geolocator.reverse([Lat, Long],language='en')
        address.append(locn.raw['address'].get('country'))
        address.append(locn.raw['address'].get('state'))
        address.append(locn.raw['address'].get('city'))
        return address
    except:
        return [None,None,None]



cleaned= df.rdd.map(lambda line: Row(Id=line[0],DisplayName=validate_display_name(line[1]),Location=line[2], AboutMe=line[3], Phone_Number=line[5],Birthdate=birthdate(line[4]),Address=locFinder(line[2])))

cleaned = cleaned.cache()
cleaned = ss.createDataFrame(cleaned, samplingRatio=0.2)

cleaned = cleaned.withColumn("Country",cleaned.Address[0])
cleaned = cleaned.withColumn("State",cleaned.Address[1])
cleaned = cleaned.withColumn("City",cleaned.Address[2])
cleaned=cleaned.drop('Address')
cleaned.show(5)


cleaned= cleaned.rdd.map(lambda line: Row(Id=line[0],DisplayName=line[1],Location=line[2],AboutMe=line[3],PhoneNumber=validate_phone_number(line[4],line[6]),BirthDate=line[5],Country=line[6],State=line[7],City=line[8]))
cleaned = ss.createDataFrame(cleaned, samplingRatio=0.2)

display = ((cleaned.filter(col("DisplayName").isNull()).count()) / cleaned.count()) * 100
aboutme = ((cleaned.filter(col("AboutMe").isNull()).count()) / cleaned.count()) * 100
birthdate = ((cleaned.filter(col("Birthdate").isNull()).count()) / cleaned.count()) * 100
phonenumber = ((cleaned.filter(col("PhoneNumber").isNull()).count()) / cleaned.count()) * 100
country = ((cleaned.filter(col("Country").isNull()).count()) / cleaned.count()) * 100

print(f"Invalid Values in DisplayName are {display}% ")
print(f"Invalid Values in AboutMe are {aboutme}% ")
print(f"Invalid Values in BirthDate are {birthdate}% ")
print(f"Invalid Values in PhoneNumber are {phonenumber}% ")
print(f"Invalid Values in Address are {country}% ")


cleaned.select([count(when(isnull(c), c)).alias(c) for c in cleaned.columns]).show()


# cleaned.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/part4")
cleaned.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part4.json")
ss.catalog.clearCache()