from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import *
import pyspark.sql.functions as F

def cleanText(df):

  df = df.withColumn('AboutMe', regexp_replace('AboutMe', '<[^>]*>', ''))
  df = df.withColumn('AboutMe', regexp_replace('AboutMe', '\n', '.'))

  def my_words(col):
    if not col:
        return
    s = ''
    if isinstance(col, str):
        s = re.sub('[^a-zA-Z]+', ' ', col).split()
    return s


  my_udf = udf(my_words, ArrayType(StringType()))

  df = df.withColumn('AboutMe', my_udf(F.col('AboutMe')))
  df = df.withColumn("AboutMe", concat_ws(" ",col("AboutMe")))
  return df

from geopy.geocoders import Nominatim

def locFinder(l):
    address = []

    try:
        geolocator = Nominatim(user_agent="sample app1")
        data = geolocator.geocode(l)
        Lat = data.point.latitude
        Long = data.point.longitude
        locn = geolocator.reverse([Lat, Long], language='en')
        address.append(locn.raw['address'].get('country'))
        address.append(locn.raw['address'].get('state'))
        address.append(locn.raw['address'].get('city'))
        print(address)
        return address
    except:
        return [None, None, None]

import datetime


def birthdate(date):

    if(date != "None"):
        year, month, day = date.split('-')
        isValidDate = True
        try:
            datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            isValidDate = False

        if (isValidDate):
            return date
        else:
            return None
    else:
        return None

import phonenumbers

# is_valid_number() method looks for the length, phone number prefix, and region to perform a complete validation

from phone_iso3166.country import *

def phoneNumValidation(phone):

  try:
    my_number = phonenumbers.parse(phone)
    c = phone_country(phone)
    # print(c)
    if(c == "IN") and int(phone[3])<6:
      return None
    if (phonenumbers.is_valid_number(my_number)):
      return phone
    else:
      return None
  except:
    return None

  #data = {"AF":"Afghanistan","AX":"Aland Islands","AL":"Albania","DZ":"Algeria","AS":"American Samoa","AD":"Andorra","AO":"Angola","AI":"Anguilla","AQ":"Antarctica","AG":"Antigua and Barbuda","AR":"Argentina","AM":"Armenia","AW":"Aruba","AU":"Australia","AT":"Austria","AZ":"Azerbaijan","BS":"Bahamas","BH":"Bahrain","BD":"Bangladesh","BB":"Barbados","BY":"Belarus","BE":"Belgium","BZ":"Belize","BJ":"Benin","BM":"Bermuda","BT":"Bhutan","BO":"Bolivia, Plurinational State of","BQ":"Bonaire, Sint Eustatius and Saba","BA":"Bosnia and Herzegovina","BW":"Botswana","BV":"Bouvet Island","BR":"Brazil","IO":"British Indian Ocean Territory","BN":"Brunei Darussalam","BG":"Bulgaria","BF":"Burkina Faso","BI":"Burundi","KH":"Cambodia","CM":"Cameroon","CA":"Canada","CV":"Cape Verde","KY":"Cayman Islands","CF":"Central African Republic","TD":"Chad","CL":"Chile","CN":"China","CX":"Christmas Island","CC":"Cocos (Keeling) Islands","CO":"Colombia","KM":"Comoros","CG":"Congo","CD":"Congo, The Democratic Republic of the","CK":"Cook Islands","CR":"Costa Rica","CI":"Côte d'Ivoire","HR":"Croatia","CU":"Cuba","CW":"Curaçao","CY":"Cyprus","CZ":"Czech Republic","DK":"Denmark","DJ":"Djibouti","DM":"Dominica","DO":"Dominican Republic","EC":"Ecuador","EG":"Egypt","SV":"El Salvador","GQ":"Equatorial Guinea","ER":"Eritrea","EE":"Estonia","ET":"Ethiopia","FK":"Falkland Islands (Malvinas)","FO":"Faroe Islands","FJ":"Fiji","FI":"Finland","FR":"France","GF":"French Guiana","PF":"French Polynesia","TF":"French Southern Territories","GA":"Gabon","GM":"Gambia","GE":"Georgia","DE":"Germany","GH":"Ghana","GI":"Gibraltar","GR":"Greece","GL":"Greenland","GD":"Grenada","GP":"Guadeloupe","GU":"Guam","GT":"Guatemala","GG":"Guernsey","GN":"Guinea","GW":"Guinea-Bissau","GY":"Guyana","HT":"Haiti","HM":"Heard Island and McDonald Islands","VA":"Holy See (Vatican City State)","HN":"Honduras","HK":"Hong Kong","HU":"Hungary","IS":"Iceland","IN":"India","ID":"Indonesia","IR":"Iran, Islamic Republic of","IQ":"Iraq","IE":"Ireland","IM":"Isle of Man","IL":"Israel","IT":"Italy","JM":"Jamaica","JP":"Japan","JE":"Jersey","JO":"Jordan","KZ":"Kazakhstan","KE":"Kenya","KI":"Kiribati","KP":"Korea, Democratic People's Republic of","KR":"Korea, Republic of","KW":"Kuwait","KG":"Kyrgyzstan","LA":"Lao People's Democratic Republic","LV":"Latvia","LB":"Lebanon","LS":"Lesotho","LR":"Liberia","LY":"Libya","LI":"Liechtenstein","LT":"Lithuania","LU":"Luxembourg","MO":"Macao","MK":"Macedonia, Republic of","MG":"Madagascar","MW":"Malawi","MY":"Malaysia","MV":"Maldives","ML":"Mali","MT":"Malta","MH":"Marshall Islands","MQ":"Martinique","MR":"Mauritania","MU":"Mauritius","YT":"Mayotte","MX":"Mexico","FM":"Micronesia, Federated States of","MD":"Moldova, Republic of","MC":"Monaco","MN":"Mongolia","ME":"Montenegro","MS":"Montserrat","MA":"Morocco","MZ":"Mozambique","MM":"Myanmar","NA":"Namibia","NR":"Nauru","NP":"Nepal","NL":"Netherlands","NC":"New Caledonia","NZ":"New Zealand","NI":"Nicaragua","NE":"Niger","NG":"Nigeria","NU":"Niue","NF":"Norfolk Island","MP":"Northern Mariana Islands","NO":"Norway","OM":"Oman","PK":"Pakistan","PW":"Palau","PS":"Palestinian Territory, Occupied","PA":"Panama","PG":"Papua New Guinea","PY":"Paraguay","PE":"Peru","PH":"Philippines","PN":"Pitcairn","PL":"Poland","PT":"Portugal","PR":"Puerto Rico","QA":"Qatar","RE":"Réunion","RO":"Romania","RU":"Russian Federation","RW":"Rwanda","BL":"Saint Barthélemy","SH":"Saint Helena, Ascension and Tristan da Cunha","KN":"Saint Kitts and Nevis","LC":"Saint Lucia","MF":"Saint Martin (French part)","PM":"Saint Pierre and Miquelon","VC":"Saint Vincent and the Grenadines","WS":"Samoa","SM":"San Marino","ST":"Sao Tome and Principe","SA":"Saudi Arabia","SN":"Senegal","RS":"Serbia","SC":"Seychelles","SL":"Sierra Leone","SG":"Singapore","SX":"Sint Maarten (Dutch part)","SK":"Slovakia","SI":"Slovenia","SB":"Solomon Islands","SO":"Somalia","ZA":"South Africa","GS":"South Georgia and the South Sandwich Islands","ES":"Spain","LK":"Sri Lanka","SD":"Sudan","SR":"Suriname","SS":"South Sudan","SJ":"Svalbard and Jan Mayen","SZ":"Swaziland","SE":"Sweden","CH":"Switzerland","SY":"Syrian Arab Republic","TW":"Taiwan, Province of China","TJ":"Tajikistan","TZ":"Tanzania, United Republic of","TH":"Thailand","TL":"Timor-Leste","TG":"Togo","TK":"Tokelau","TO":"Tonga","TT":"Trinidad and Tobago","TN":"Tunisia","TR":"Turkey","TM":"Turkmenistan","TC":"Turks and Caicos Islands","TV":"Tuvalu","UG":"Uganda","UA":"Ukraine","AE":"United Arab Emirates","GB":"United Kingdom","US":"United States","UM":"United States Minor Outlying Islands","UY":"Uruguay","UZ":"Uzbekistan","VU":"Vanuatu","VE":"Venezuela, Bolivarian Republic of","VN":"Viet Nam","VG":"Virgin Islands, British","VI":"Virgin Islands, U.S.","WF":"Wallis and Futuna","YE":"Yemen","ZM":"Zambia","ZW":"Zimbabwe"}
  # try:
  #     my_number = phonenumbers.parse(phone)
  #     c = phone_country(phone)
  #     if (data[c] == country):
  #         return phone
  #     else:
  #         return None
  # except:
  #     return None

import re

def displayNameValidation(displayname):
  regexp = re.compile('[^\w\s.]')
  if regexp.search(displayname):
    return None
  else:
    return displayname



# from Code.legal.PY.cleanAddress import *
# from Code.legal.PY.cleanAboutMe import *
# from Code.legal.PY.validateBirthdate import *
# from Code.legal.PY.validatePhoneNo import *

master = 'local'
appName = 'PySpark_CleanAddress4'
config = SparkConf().setAppName(appName).setMaster(master)
spark = SparkSession.builder.appName('MySparkStreamingSession4')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .master('local')\
    .getOrCreate()

print(spark.sparkContext.getConf().get("spark.serializer"))
print('Done')

if spark:
    print(spark.sparkContext.appName)
else:
    print('Could not initialise pyspark session')

df = pd.read_csv("/home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/EmailAttachments/part_1.csv")

df_schema = StructType([StructField('Id', IntegerType(), True),
                        StructField('DisplayName', StringType(), True),
                        StructField('AccountId', StringType(), True),
                        StructField('Reputation', StringType(), True),
                        StructField('CreationDate', StringType(), True),
                        StructField('WebsiteUrl', StringType(), True),
                        StructField('Location', StringType(), True),
                        StructField('AboutMe', StringType(), True),
                        StructField('UpVotes', StringType(), True),
                        StructField('DownVotes', StringType(), True),
                        StructField('Birthdate', StringType(), True),
                        StructField('PhoneNumber', StringType(), True)])
data = spark.createDataFrame(df, schema = df_schema)

data = data.select(["Id", "DisplayName", "Location", "AboutMe", "Birthdate", "PhoneNumber"])
data = data.sample(False, 0.002, seed=100)
print("Total records = ", data.count())    # Total numbers of records
data.show(5)



# Cleaning AboutMe column for tags, quotations
data = cleanText(data)

# Validating and cleaning Birthdate, Creating Country, State, City columns from Locations
data = data.withColumn("Birthdate", data.Birthdate.cast(DateType()))
df_cleaned = data.rdd.map(lambda line: Row(Id=line[0], DisplayName=line[1], Location=line[2], Address=locFinder(line[2]),
                                           AboutMe=line[3], Birthdate=birthdate(str(line[4])), PhoneNumber=line[5]))
df_cleaned = spark.createDataFrame(df_cleaned, samplingRatio=0.2)

df_cleaned = df_cleaned.cache()
df_cleaned.show(5)


# In newly obtained Address column, Country, State, City are stored in list. So creating a seperate column for each
df_cleaned = df_cleaned.withColumn("Country", df_cleaned.Address[0])
df_cleaned = df_cleaned.withColumn("State", df_cleaned.Address[1])
df_cleaned = df_cleaned.withColumn("City", df_cleaned.Address[2])
df_cleaned = df_cleaned.drop("Address")

# Cleaning PhoneNumber
final = df_cleaned.rdd.map(lambda line: Row(Id=line[0], DisplayName=displayNameValidation(line[1]), Location=line[2], AboutMe=line[3],
                                            Birthdate=line[4], PhoneNumber=phoneNumValidation(line[5]),
                                            Country=line[6], State=line[7], City=line[8]))
final = spark.createDataFrame(final, samplingRatio=0.2)


final.select('Id','DisplayName','AboutMe','BirthDate','PhoneNumber','Location','Country','State','City').show()
# final.write.csv("/content/drive/MyDrive/Colab Notebooks/Project/legal/CSVclean_dataset_01_part4", lineSep="\n", header=True)
# final.write.json("/content/drive/MyDrive/Colab Notebooks/Project/legal/ipynb/sampleCSV_part1", lineSep="\n")

# df = spark.read.csv("/home/theshree/Documents/DBDA/Project/Code/legal/CSVclean_dataset_01_part1/part-00000-342ee147-a6e2-46ed-bfbd-e85e4333c3ba-c000.csv", header=True)

# Printing null values in columns
print(final.select([count(when(isnull(c), c)).alias(c) for c in final.columns]).show())

# Percentages
display = ((final.filter(col("DisplayName").isNull()).count())/final.count())*100
aboutme = ((final.filter(col("AboutMe").isNull()).count())/final.count())*100
birthdate = ((final.filter(col("BirthDate").isNull()).count())/final.count())*100
phonenumber = ((final.filter(col("PhoneNumber").isNull()).count())/final.count())*100
country = ((final.filter(col("Country").isNull()).count())/final.count())*100

final.rdd.coalesce(1)
# final.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/part1")
final.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part1.json")
print(f"Invalid Values in DisplayName are {display}% ")
print(f"Invalid Values in AboutMe are {aboutme}% ")
print(f"Invalid Values in BirthDate are {birthdate}% ")
print(f"Invalid Values in PhoneNumber are {phonenumber}% ")
print(f"Invalid Values in Address are {country}% ")

spark.catalog.clearCache()

