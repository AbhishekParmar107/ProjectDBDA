from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.functions import monotonically_increasing_id, udf,isnan, when, count, col,isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, FloatType,DateType, Row
from phone_iso3166.country import *
import warnings
import re
warnings.filterwarnings("ignore")
from datetime import datetime

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Final Project") \
    .getOrCreate()

part5= spark.read.option('multiline','true').json('file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/samplepart5.json')
part5.show()
# schema = part.schema
# part = spark.read.option('multiline','true').schema(schema).json('file:////home/sanket/project/5th/part_05.json')
# part5=part.limit(50)
part5=part5.withColumnRenamed("Phone Number","PhoneNumber")
part5.createGlobalTempView('part_05')
part5 = spark.sql("Select CONCAT(First_Name, ' ' , Last_Name) as DisplayName, address, PhoneNumber, Birthdate, AboutMe from global_temp.part_05")

##################################################################################
def division():
    import pandas as pd
    df = pd.read_excel("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/Town_Codes_2001.xls")
    df = df.replace('[^\w\s]', '', regex=True)
    city = df['City/Town'].str.strip().tolist()
    city = [s.lower().strip() for s in city]

    state = df['State'].str.strip().tolist()
    state = [s.lower().strip() for s in state]

    district = df['District'].str.strip().tolist()
    district = [s.lower().strip() for s in district]

    city = set(city)
    city.add("mumbai")
    state = set(state)
    district = set(district)

    same = city.intersection(district)
    city = city - same
    same1 = city.intersection(state)
    city = city - same1

    dict1 = list(zip(city, state))
    cities = {}
    for i in dict1:
        cities[i[0]] = i[1]
    cities['Mumbai'] = 'Maharashtra'
    return city,district,state,cities

city,district,state,cities = division()

#################################################################

def address(location):
    loc = location.split(",")
    loc1=[]
    for i in loc:
        loc1.append(i.lower().strip())
    loc = set(loc)
    common1 = city.intersection(loc1)
    common2 = district.intersection(loc1)
    common3 = state.intersection(loc1)
    final = ', '.join(common1)+' '+', '.join(common2)+" "+', '.join(common3)
    final= final.strip()
    if(len(common1)>1 or len(common2)>1 or len(common3)>1):
        return " "
    elif cities.get(final) is not None:
        final = final+" "+str(cities.get(final))
        print(final)
        return final
    else:
        return final

#######################################################

from geopy.geocoders import Nominatim
count = 0
def locFinder(l):
    global count
    count = count+1
    if count%10==0:
        print(count)

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




#print(locFinder(address("dasdas,india,dasdsa,panvel")))
part5= part5.rdd.map(lambda line: Row(DisplayName=line[0], Location=line[1], PhoneNumber=line[2],Birthdate=line[3], AboutMe=line[4], Address=locFinder(address(line[1])))).toDF().cache()
#address, PhoneNumber, Birthdate, AboutMe
part5=part5.withColumn('Country',
                       part5.Address[0])
part5=part5.withColumn('State',
                       part5.Address[1])
part5=part5.withColumn('City',
                       part5.Address[2])
# part5['country'] =part5.select('location').rdd.flatMap(lambda x: x)[0]
# part5.write.mode('overwrite').json('/home/sanket/project/5th/cleaned.json')
# part5 = spark.read.json('/home/sanket/project/DATA_INGESTION/Ingestion/sample1.json')
# schema = part5.schema
# part5 = spark.read.schema(schema).json('/home/sanket/project/DATA_INGESTION/Ingestion/part_5_cleaned.json')
# part5.select([count(when(isnull(c), c)).alias(c) for c in part5.columns]).show()
part5=part5.select('DisplayName','AboutMe','Birthdate','PhoneNumber','Location','Country','State','City')
new_column_udf = udf(lambda Birthdate: None if Birthdate == "Invalid" else Birthdate, StringType())
part5 = part5.withColumn("Birthdate", new_column_udf(part5.Birthdate))
# part5.select([count(when(isnull(c), c)).alias(c) for c in part5.columns]).show()
# part5.show(truncate=False)

data = {"AF":"Afghanistan","AX":"Aland Islands","AL":"Albania","DZ":"Algeria","AS":"American Samoa","AD":"Andorra","AO":"Angola","AI":"Anguilla","AQ":"Antarctica","AG":"Antigua and Barbuda","AR":"Argentina","AM":"Armenia","AW":"Aruba","AU":"Australia","AT":"Austria","AZ":"Azerbaijan","BS":"Bahamas","BH":"Bahrain","BD":"Bangladesh","BB":"Barbados","BY":"Belarus","BE":"Belgium","BZ":"Belize","BJ":"Benin","BM":"Bermuda","BT":"Bhutan","BO":"Bolivia, Plurinational State of","BQ":"Bonaire, Sint Eustatius and Saba","BA":"Bosnia and Herzegovina","BW":"Botswana","BV":"Bouvet Island","BR":"Brazil","IO":"British Indian Ocean Territory","BN":"Brunei Darussalam","BG":"Bulgaria","BF":"Burkina Faso","BI":"Burundi","KH":"Cambodia","CM":"Cameroon","CA":"Canada","CV":"Cape Verde","KY":"Cayman Islands","CF":"Central African Republic","TD":"Chad","CL":"Chile","CN":"China","CX":"Christmas Island","CC":"Cocos (Keeling) Islands","CO":"Colombia","KM":"Comoros","CG":"Congo","CD":"Congo, The Democratic Republic of the","CK":"Cook Islands","CR":"Costa Rica","CI":"Côte d'Ivoire","HR":"Croatia","CU":"Cuba","CW":"Curaçao","CY":"Cyprus","CZ":"Czech Republic","DK":"Denmark","DJ":"Djibouti","DM":"Dominica","DO":"Dominican Republic","EC":"Ecuador","EG":"Egypt","SV":"El Salvador","GQ":"Equatorial Guinea","ER":"Eritrea","EE":"Estonia","ET":"Ethiopia","FK":"Falkland Islands (Malvinas)","FO":"Faroe Islands","FJ":"Fiji","FI":"Finland","FR":"France","GF":"French Guiana","PF":"French Polynesia","TF":"French Southern Territories","GA":"Gabon","GM":"Gambia","GE":"Georgia","DE":"Germany","GH":"Ghana","GI":"Gibraltar","GR":"Greece","GL":"Greenland","GD":"Grenada","GP":"Guadeloupe","GU":"Guam","GT":"Guatemala","GG":"Guernsey","GN":"Guinea","GW":"Guinea-Bissau","GY":"Guyana","HT":"Haiti","HM":"Heard Island and McDonald Islands","VA":"Holy See (Vatican City State)","HN":"Honduras","HK":"Hong Kong","HU":"Hungary","IS":"Iceland","IN":"India","ID":"Indonesia","IR":"Iran, Islamic Republic of","IQ":"Iraq","IE":"Ireland","IM":"Isle of Man","IL":"Israel","IT":"Italy","JM":"Jamaica","JP":"Japan","JE":"Jersey","JO":"Jordan","KZ":"Kazakhstan","KE":"Kenya","KI":"Kiribati","KP":"Korea, Democratic People's Republic of","KR":"Korea, Republic of","KW":"Kuwait","KG":"Kyrgyzstan","LA":"Lao People's Democratic Republic","LV":"Latvia","LB":"Lebanon","LS":"Lesotho","LR":"Liberia","LY":"Libya","LI":"Liechtenstein","LT":"Lithuania","LU":"Luxembourg","MO":"Macao","MK":"Macedonia, Republic of","MG":"Madagascar","MW":"Malawi","MY":"Malaysia","MV":"Maldives","ML":"Mali","MT":"Malta","MH":"Marshall Islands","MQ":"Martinique","MR":"Mauritania","MU":"Mauritius","YT":"Mayotte","MX":"Mexico","FM":"Micronesia, Federated States of","MD":"Moldova, Republic of","MC":"Monaco","MN":"Mongolia","ME":"Montenegro","MS":"Montserrat","MA":"Morocco","MZ":"Mozambique","MM":"Myanmar","NA":"Namibia","NR":"Nauru","NP":"Nepal","NL":"Netherlands","NC":"New Caledonia","NZ":"New Zealand","NI":"Nicaragua","NE":"Niger","NG":"Nigeria","NU":"Niue","NF":"Norfolk Island","MP":"Northern Mariana Islands","NO":"Norway","OM":"Oman","PK":"Pakistan","PW":"Palau","PS":"Palestinian Territory, Occupied","PA":"Panama","PG":"Papua New Guinea","PY":"Paraguay","PE":"Peru","PH":"Philippines","PN":"Pitcairn","PL":"Poland","PT":"Portugal","PR":"Puerto Rico","QA":"Qatar","RE":"Réunion","RO":"Romania","RU":"Russian Federation","RW":"Rwanda","BL":"Saint Barthélemy","SH":"Saint Helena, Ascension and Tristan da Cunha","KN":"Saint Kitts and Nevis","LC":"Saint Lucia","MF":"Saint Martin (French part)","PM":"Saint Pierre and Miquelon","VC":"Saint Vincent and the Grenadines","WS":"Samoa","SM":"San Marino","ST":"Sao Tome and Principe","SA":"Saudi Arabia","SN":"Senegal","RS":"Serbia","SC":"Seychelles","SL":"Sierra Leone","SG":"Singapore","SX":"Sint Maarten (Dutch part)","SK":"Slovakia","SI":"Slovenia","SB":"Solomon Islands","SO":"Somalia","ZA":"South Africa","GS":"South Georgia and the South Sandwich Islands","ES":"Spain","LK":"Sri Lanka","SD":"Sudan","SR":"Suriname","SS":"South Sudan","SJ":"Svalbard and Jan Mayen","SZ":"Swaziland","SE":"Sweden","CH":"Switzerland","SY":"Syrian Arab Republic","TW":"Taiwan, Province of China","TJ":"Tajikistan","TZ":"Tanzania, United Republic of","TH":"Thailand","TL":"Timor-Leste","TG":"Togo","TK":"Tokelau","TO":"Tonga","TT":"Trinidad and Tobago","TN":"Tunisia","TR":"Turkey","TM":"Turkmenistan","TC":"Turks and Caicos Islands","TV":"Tuvalu","UG":"Uganda","UA":"Ukraine","AE":"United Arab Emirates","GB":"United Kingdom","US":"United States","UM":"United States Minor Outlying Islands","UY":"Uruguay","UZ":"Uzbekistan","VU":"Vanuatu","VE":"Venezuela, Bolivarian Republic of","VN":"Viet Nam","VG":"Virgin Islands, British","VI":"Virgin Islands, U.S.","WF":"Wallis and Futuna","YE":"Yemen","ZM":"Zambia","ZW":"Zimbabwe"}

def phoneNumValidation(phone,country):
    try:
        c = phone_country(phone)
        if data[c] == country:
            return phone
        else:
            return None
    except:
        return None

def displayNameValidation(displayname):
    regexp = re.compile('[^a-zA-Z\s.]')
    if regexp.search(displayname):
        return None
    else:
        return displayname

def birthdate(date):
    format = "%Y-%m-%d"
    try:
        if bool(datetime.strptime(date, format))==False:
            return "Invalid"
        else:
            year = date.split("-")[0]
            if 2012 >year < 1950:
                return date
            return "Invalid"
    except:
        return "Invalid"
part5 = part5.cache()
part5= part5.rdd.map(lambda line: Row(DisplayName=displayNameValidation(line[0]), AboutMe=line[1], BirthDate=line[2],PhoneNumber=phoneNumValidation(line[3],line[5]), Location=line[4], Country=line[5], State=line[6],City=line[7])).cache()
part5 = spark.createDataFrame(part5,samplingRatio=0.2)
part5=part5.withColumn("ID", monotonically_increasing_id())
# part5.write.mode('overwrite').json('/home/sanket/project/5th/combined')
part5.select('ID','DisplayName','AboutMe','BirthDate','PhoneNumber','Location','Country','State','City').show()

display=((part5.filter(col("DisplayName").isNull()).count())/part5.count())*100
aboutme=((part5.filter(col("AboutMe").isNull()).count())/part5.count())*100
birthdate=((part5.filter(col("BirthDate").isNull()).count())/part5.count())*100
phonenumber=((part5.filter(col("PhoneNumber").isNull()).count())/part5.count())*100
country=((part5.filter(col("Country").isNull()).count())/part5.count())*100

print(f"Invalid Values in DisplayName are {display}% ")
print(f"Invalid Values in AboutMe are {aboutme}% ")
print(f"Invalid Values in BirthDate are {birthdate}% ")
print(f"Invalid Values in PhoneNumber are {phonenumber}% ")
print(f"Invalid Values in Address are {country}% ")

# part5.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/part4")

part5.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part5.json")
spark.catalog.clearCache()