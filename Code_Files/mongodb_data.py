from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import warnings

warnings.filterwarnings("ignore")

database = "CdacProjcect"
collection = "UsersData"
connectionString = ("mongodb+srv://cdac:cdac@cluster0.nma2z.mongodb.net/CdacProjcect?retryWrites=true&w=majority")
spark = SparkSession \
    .builder \
    .config('spark.mongodb.input.uri', connectionString) \
    .config('spark.mongodb.output.uri', connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

spark.sparkContext.addPyFile("/home/rahulw/DBDA_HOME/spark-3.2.0-bin-hadoop3.2/jars/mongo-spark-connector_2.11-2.4.1.jar")
# Reading from MongoDB
df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", connectionString) \
    .option("database", database) \
    .option("collection", collection) \
    .load()

users = df.select('_id', 'Id', 'DisplayName', 'Cleaned_AboutMe', 'Location', 'BirthDate', 'City', 'State', 'Country',
                  'phone')
# print(users.dtypes)
# repmoving special symbols used in phone numbers
users = users.withColumn('phone', regexp_replace('phone', '[-+]', ''))
users.show(5)

country_df = spark.read.json(
    "/home/rahulw/PycharmProjects/Final_Project/Ingestion/FromDatabase/country_data/country_codes.json")
country_df.show(3)
# # removing -+ from country_code as
country_df = country_df.withColumn('Country_code', regexp_replace('Country_code', '[-+]', ''))
#
#
final_users = users.join(country_df, users["Country"] == country_df["Country_name"]).select('_id','Id', 'DisplayName',
                                                                                            'Cleaned_AboutMe',
                                                                                            'Location', 'BirthDate',
                                                                                            'phone', 'City', 'Country',
                                                                                            'State', 'Country_code')
final_users.show()

import re

regexp = re.compile('[^\w\s.]')


def display_name_validation(name):
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


prog = re.compile("^(19|20)\d\d-(0[1-9]|1[0-2])-([1-9]|0[1-9]|[12][0-9]|3[01])$")


def birthdate_val(date):
    """
    (19|20)\d\d-  ==>  First two digits of year should be 19 or 20 followed by any two digits and then hyphen sign i.e. "-" as seperator is used
    (0[1-9]|1[0-2]) ==> Month Validation is 0[1-9]  ==> ex. 01, 02,..., 09
                                            1[0-2]  ==> ex. 10, 11, 12

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


from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# user defined functions

udf_phone_val = udf(phone_validation, StringType())
udf_birthdate_val = udf(birthdate_val, StringType())
udf_name_val = udf(display_name_validation, StringType())

final_users = final_users.withColumn("phone", udf_phone_val(final_users.phone, final_users.Country_code))

final_users = final_users.withColumn("BirthDate", udf_birthdate_val(final_users.BirthDate))

final_users = final_users.withColumn("DisplayName", udf_name_val(final_users.DisplayName))

cleaned = final_users.select('Id', 'DisplayName', 'Cleaned_AboutMe', 'Location', 'BirthDate', 'phone', 'City',
                                 'Country', 'State')




name_pairs = [('Cleaned_AboutMe', 'AboutMe'),('phone','PhoneNumber')]
for old_name, new_name in name_pairs:
    cleaned = cleaned.withColumnRenamed(old_name,new_name)
cleaned.show(5)

# cleaned.select([count(when(isnull(c), c)).alias(c) for c in final_users.columns]).show()

# # final_users.write.mode("overwrite").json("/home/rahulw/PycharmProjects/Final_Project/Ingestion/Final/output",lineSep=",")
# final_users.show(5)
display = ((cleaned.filter(col("DisplayName").isNull()).count()) / cleaned.count()) * 100
aboutme = ((cleaned.filter(col("AboutMe").isNull()).count()) / cleaned.count()) * 100
birthdate = ((cleaned.filter(col("BirthDate").isNull()).count()) / cleaned.count()) * 100
phonenumber = ((cleaned.filter(col("PhoneNumber").isNull()).count()) / cleaned.count()) * 100
country = ((cleaned.filter(col("Country").isNull()).count()) / cleaned.count()) * 100

print(f"Invalid Values in DisplayName are {display}% ")
print(f"Invalid Values in AboutMe are {aboutme}% ")
print(f"Invalid Values in BirthDate are {birthdate}% ")
print(f"Invalid Values in PhoneNumber are {phonenumber}% ")
print(f"Invalid Values in Address are {country}% ")

# cleaned.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/mongo_part6")
cleaned.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/mongo_part6.json")
"""
+---+-----------+---------------+--------+---------+-----+----+-------+-----+
| Id|DisplayName|Cleaned_AboutMe|Location|BirthDate|phone|City|Country|State|
+---+-----------+---------------+--------+---------+-----+----+-------+-----+
|  0|        196|              0|       0|      309|   14|5099|      0| 1882|
+---+-----------+---------------+--------+---------+-----+----+-------+-----+


Invalid Values in DisplayName are 1.2838988602122363% 
Invalid Values in AboutMe are 0.0% 
Invalid Values in BirthDate are 2.024105856150924% 
Invalid Values in PhoneNumber are 0.09170706144373117% 
Invalid Values in Address are 0.0% 

"""


