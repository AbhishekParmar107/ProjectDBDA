from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, Row, ArrayType, StringType
import pandas as pd
import json
import datetime
import re

master = 'local'
appName = 'Data Cleaning and Validation'

spark = SparkSession.builder.appName(appName).getOrCreate()

if spark.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

raw_data = spark.read.option("multiline", "true").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/sample_part2.json")
raw_data.show(5)
print("Number of sample records:",raw_data.count())

users_data = raw_data.select('Id', 'DisplayName', 'Location', 'AboutMe', 'Birthdate', 'PhoneNumber')

# users_data.show(5)
"""
# Cleaning AboutMe column
from bs4 import BeautifulSoup

# For removing stopwords
from gensim.parsing.preprocessing import remove_stopwords

# Function to remove html tags and convert data into text format

def text_cleaning(mytext):
    if (isinstance(mytext, str)):
        soup = BeautifulSoup(mytext)
        raw = soup.get_text()
        raw = raw.replace("\n", " ").strip()
        return remove_stopwords(raw)
    else:
        return "NA"


# we have compared performance of beautiful soup funcion and regex. we found that regex is much faster
# so we choosed to go with regex.
"""

# AboutMe html tags are removed using regex
from pyspark.sql.functions import regexp_replace, count, when, isnull, col, concat_ws, udf
import pyspark.sql.functions as F

df = users_data.withColumn('AboutMe', regexp_replace('AboutMe', '<[^>]*>', ''))
df = df.withColumn('AboutMe', regexp_replace('AboutMe', '\n', '.'))

def my_words(col):
    if not col:
        return
    s = ''
    if isinstance(col, str):
        s = re.sub('[^a-zA-Z0-9]+', ' ', col).split()
    return s


my_udf = udf(my_words, ArrayType(StringType()))

df = df.withColumn('AboutMe', my_udf(F.col('AboutMe')))
df = df.withColumn("AboutMe", concat_ws(" ",col("AboutMe")))

"""
from gensim.parsing.preprocessing import remove_stopwords
def rm_stopwords(l):
    return remove_stopwords(l)

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# user defined functions
udf_remove_stopword = udf(rm_stopwords,StringType())
df.show()
df = df.withColumn("AboutMe", udf_remove_stopword(df.AboutMe))
"""

from geopy.geocoders import Nominatim

def locFinder(l):
    try:
        geolocator = Nominatim(user_agent="sample app")
        address = []
        data = geolocator.geocode(l)
        lat = data.point.latitude
        long = data.point.longitude
        locn = geolocator.reverse([lat, long], language='en')
        address.append(locn.raw['address'].get('country'))
        address.append(locn.raw['address'].get('state'))
        address.append(locn.raw['address'].get('city'))
        return address
    except:
        return [None, None, None]


# data = df.dropna()

# print(locFinder("pune"))
cleaned = users_data.rdd.map(lambda line: Row(Id=line[0],
                                        DisplayName=line[1],
                                        Location=line[2],
                                        Address=locFinder(line[2]),
                                        Cleaned_AboutMe=line[3],
                                        BirthDate=line[4],
                                        phone=line[5]))
cleaned = spark.createDataFrame(cleaned,samplingRatio=0.2)
cleaned = cleaned.withColumn("Country", cleaned.Address[0])
cleaned = cleaned.withColumn("State", cleaned.Address[1])
users = cleaned.withColumn("City", cleaned.Address[2])
users = users.drop("Address")
users.show(5)
users = users.cache()
# Now, users dataframe will have City,State,Country as well

# reading the data from the file having country name abbreviations and country name
with open('/home/rahulw/PycharmProjects/Final_Project/Ingestion/Data/phone_validate/names.txt') as f:
    data = f.read()

# print("Data type before reconstruction : ", type(data))

# reconstructing the data as a dictionary
names_dict = json.loads(data)
names_df = pd.DataFrame(list(names_dict.items()), columns=['Country_abr', 'Country_name'])

# reading the data from the file having country abbreviations and country code
with open('/home/rahulw/PycharmProjects/Final_Project/Ingestion/Data/phone_validate/phone.json') as f:
    data = f.read()

phone_dict = json.loads(data)
phone_df = pd.DataFrame(list(phone_dict.items()), columns=['Country_abr', 'Country_code'])

# merging names_df and phone_df to get final_df . we will get ['Country_abr','Country_name', 'Country_code'] in a dataframe
final_df = names_df.merge(phone_df, on='Country_abr')

country_df = spark.createDataFrame(final_df)

# removing -+ from country_code as
country_df = country_df.withColumn('Country_code', regexp_replace('Country_code', '[-+]', ''))

# removing special symbols used in phone numbers
users = users.withColumn('phone', regexp_replace('phone', '[-+]', ''))

final_users = users.join(country_df, users["Country"] == country_df["Country_name"]).select('Id', 'DisplayName',
                                                                                            'Cleaned_AboutMe',
                                                                                            'Location', 'BirthDate',
                                                                                            'phone', 'City', 'Country',
                                                                                            'State', 'Country_code')


def display_name_validation(name):
    regexp = re.compile('[^\w\s.]')
    if regexp.search(name):
        return None
    else:
        return name


def phone_validation(phone, country_code):
    """
        :param phone: existing phone number
        :param country_code: is starts with country code or not
        :return: phone number if matches or None
    """
    try:
        if phone.startswith(country_code):
            return phone
        else:
            return None
    except:
        return None


def birthdate(date):
    """
    we are assuming that birth year of user is between 1950 to 2012
    Extracting year,month,day from date string, if any exception comes then date will be considered as invalid.
    :param date: date as string
    :return: date if valid else None
    """
    try:
        year = date.split('-')[0]
        datetime.datetime(int(year))
        if (1950 < int(year) < 2012):
            return date
        return None
    except:
        return None


prog = re.compile("^(19|20)\d\d[-/;](0[1-9]|1[0-2])[-/;]([1-9]|0[1-9]|[12][0-9]|3[01])$")

def birthdate_val(date):
    """
    (19|20)\d\d-  ==>  First two digits of year should be 19 or 20 followed by any two digits and then hyphen sign i.e. "-" as seperator is used
    (0[1-9]|1[0-2]) ==> Month Validation is 0[1-9] means 0 and number between 1 to 9 ==> ex. 01, 02,..., 09
       | ==> Or                             1[0-2] means 1 and number between 0 to 2 ==> ex. 10, 11, 12
       $ ==> end of string

    :param date: date as string
    :return: date if valid else None
    """
    try:
        year = date.split('-')[0]
        if ((1950 < int(year) < 2012) and (prog.match(date) != None)):
            return date
        else:
            return None
    except:
        return None


cleaned = final_users.rdd.map(lambda line: Row(Id=line[0],
                                               DisplayName=display_name_validation(line[1]),
                                               AboutMe=line[2],
                                               Location=line[3],
                                               BirthDate=birthdate_val(line[4]),
                                               PhoneNumber=phone_validation(line[5], line[9]),
                                               City=line[6],
                                               State=line[8],
                                               Country=line[7])).toDF()
cleaned.show(5)
cleaned.select([count(when(isnull(c), c)).alias(c) for c in cleaned.columns]).show()

# cleaned_sorted = cleaned.withColumn("Id", cleaned['Id'].cast(IntegerType()))
#
# cleaned_sorted = cleaned_sorted.sort("Id")
# cleaned_sorted.show(10)
# cleaned_sorted.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part_2.json")
# Percentages
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

# cleaned.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/part2")
cleaned.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part2.json")
spark.catalog.clearCache()
# cleaned_sorted.write.mode("overwrite").json("/home/rahulw/PycharmProjects/Final_Project/Ingestion/Final/output",lineSep=",\n")
