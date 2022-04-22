import pandas as pd
import pyspark.sql.catalog
from pyspark.sql.functions import *
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession
from datetime import datetime
from phone_iso3166.country import *
from pyspark.sql.functions import count, when, isnull
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType, Row


# initialising the spark session
appName = "Data Ingestion - Project - Cleaning"
sparkSession = SparkSession.builder \
    .master("local") \
    .appName(appName) \
    .getOrCreate()
if SparkSession.sparkContext:
    print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    print(f'AppName: {sparkSession.sparkContext.appName}')
    print(f'Master: {sparkSession.sparkContext.master}')
    print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
else:
    print('Could not initialise pyspark session')

schem = StructType([StructField('No', IntegerType(), True),
                    StructField('Reputation', StringType(), True),
                    StructField('DisplayName', StringType(), True),
                    StructField('LastAccessDate', StringType(), True),
                    StructField('Location', StringType(), True),
                    StructField('UpVotes', IntegerType(), True),
                    StructField('DownVotes', IntegerType(), True),
                    StructField('AccountID', FloatType(), True),
                    StructField('AboutMe', StringType(), True),
                    StructField('Birthdate', StringType(), True),
                    StructField('PhoneNo', StringType(),True)])
df = pd.read_csv("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/sample_PART_3.csv")
print(df.columns)
df = sparkSession.createDataFrame(df, schema=schem)

df = df.withColumn('AboutMe', regexp_replace('AboutMe', '[\\n]', " "))
df = df.withColumn('AboutMe', regexp_replace('AboutMe', '"', "'"))
df = df.limit(20)

# df.show(5)

# FUNCTION TO EXTRACT COUNTRY, STATE AND CITY FROM THE 'LOCATION'

def locFinder(l):
    try:
        geolocator = Nominatim(user_agent="sample app")
        address = []
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
        return (None, None, None)


df_cleaned = df.rdd.map(lambda line: Row(Id=line[0],
                                         DisplayName=line[2],
                                         Location=line[4],
                                         Address= locFinder(line[4]),
                                         Cleaned_AboutMe=line[8],
                                         BirthDate=line[9],
                                         PhoneNo = line[10])).toDF()
df_cleaned = df_cleaned.cache()

df_cleaned = df_cleaned.withColumn("Country",df_cleaned.Address[0])
df_cleaned = df_cleaned.withColumn("State",df_cleaned.Address[1])
df_cleaned = df_cleaned.withColumn("City",df_cleaned.Address[2])

# *************************** VALIDATION ***********************************

# -------------------------- BIRTHDATE VALIDATION --------------------------
def dateValidation(date):
    try:
        if bool(datetime.strptime(date, "%Y-%m-%d"))==False:
            return None
        else:
            year = int(date.split("-")[0])
            if year <= 1950 or year >=2012:
                return None
            else:
                return date
    except:
        return None


# -------------------------- PHONE NUMBER VALIDATION --------------------------
data = {"AF": "Afghanistan", "AX": "Aland Islands", "AL": "Albania", "DZ": "Algeria", "AS": "American Samoa",
        "AD": "Andorra", "AO": "Angola", "AI": "Anguilla", "AQ": "Antarctica", "AG": "Antigua and Barbuda",
        "AR": "Argentina", "AM": "Armenia", "AW": "Aruba", "AU": "Australia", "AT": "Austria", "AZ": "Azerbaijan",
        "BS": "Bahamas", "BH": "Bahrain", "BD": "Bangladesh", "BB": "Barbados", "BY": "Belarus", "BE": "Belgium",
        "BZ": "Belize", "BJ": "Benin", "BM": "Bermuda", "BT": "Bhutan", "BO": "Bolivia, Plurinational State of",
        "BQ": "Bonaire, Sint Eustatius and Saba", "BA": "Bosnia and Herzegovina", "BW": "Botswana",
        "BV": "Bouvet Island", "BR": "Brazil", "IO": "British Indian Ocean Territory", "BN": "Brunei Darussalam",
        "BG": "Bulgaria", "BF": "Burkina Faso", "BI": "Burundi", "KH": "Cambodia", "CM": "Cameroon", "CA": "Canada",
        "CV": "Cape Verde", "KY": "Cayman Islands", "CF": "Central African Republic", "TD": "Chad", "CL": "Chile",
        "CN": "China", "CX": "Christmas Island", "CC": "Cocos (Keeling) Islands", "CO": "Colombia", "KM": "Comoros",
        "CG": "Congo", "CD": "Congo, The Democratic Republic of the", "CK": "Cook Islands", "CR": "Costa Rica",
        "CI": "Côte d'Ivoire", "HR": "Croatia", "CU": "Cuba", "CW": "Curaçao", "CY": "Cyprus",
        "CZ": "Czech Republic", "DK": "Denmark", "DJ": "Djibouti", "DM": "Dominica", "DO": "Dominican Republic",
        "EC": "Ecuador", "EG": "Egypt", "SV": "El Salvador", "GQ": "Equatorial Guinea", "ER": "Eritrea",
        "EE": "Estonia", "ET": "Ethiopia", "FK": "Falkland Islands (Malvinas)", "FO": "Faroe Islands", "FJ": "Fiji",
        "FI": "Finland", "FR": "France", "GF": "French Guiana", "PF": "French Polynesia",
        "TF": "French Southern Territories", "GA": "Gabon", "GM": "Gambia", "GE": "Georgia", "DE": "Germany",
        "GH": "Ghana", "GI": "Gibraltar", "GR": "Greece", "GL": "Greenland", "GD": "Grenada", "GP": "Guadeloupe",
        "GU": "Guam", "GT": "Guatemala", "GG": "Guernsey", "GN": "Guinea", "GW": "Guinea-Bissau", "GY": "Guyana",
        "HT": "Haiti", "HM": "Heard Island and McDonald Islands", "VA": "Holy See (Vatican City State)",
        "HN": "Honduras", "HK": "Hong Kong", "HU": "Hungary", "IS": "Iceland", "IN": "India", "ID": "Indonesia",
        "IR": "Iran, Islamic Republic of", "IQ": "Iraq", "IE": "Ireland", "IM": "Isle of Man", "IL": "Israel",
        "IT": "Italy", "JM": "Jamaica", "JP": "Japan", "JE": "Jersey", "JO": "Jordan", "KZ": "Kazakhstan",
        "KE": "Kenya", "KI": "Kiribati", "KP": "Korea, Democratic People's Republic of", "KR": "Korea, Republic of",
        "KW": "Kuwait", "KG": "Kyrgyzstan", "LA": "Lao People's Democratic Republic", "LV": "Latvia",
        "LB": "Lebanon", "LS": "Lesotho", "LR": "Liberia", "LY": "Libya", "LI": "Liechtenstein", "LT": "Lithuania",
        "LU": "Luxembourg", "MO": "Macao", "MK": "Macedonia, Republic of", "MG": "Madagascar", "MW": "Malawi",
        "MY": "Malaysia", "MV": "Maldives", "ML": "Mali", "MT": "Malta", "MH": "Marshall Islands",
        "MQ": "Martinique", "MR": "Mauritania", "MU": "Mauritius", "YT": "Mayotte", "MX": "Mexico",
        "FM": "Micronesia, Federated States of", "MD": "Moldova, Republic of", "MC": "Monaco", "MN": "Mongolia",
        "ME": "Montenegro", "MS": "Montserrat", "MA": "Morocco", "MZ": "Mozambique", "MM": "Myanmar",
        "NA": "Namibia", "NR": "Nauru", "NP": "Nepal", "NL": "Netherlands", "NC": "New Caledonia",
        "NZ": "New Zealand", "NI": "Nicaragua", "NE": "Niger", "NG": "Nigeria", "NU": "Niue",
        "NF": "Norfolk Island", "MP": "Northern Mariana Islands", "NO": "Norway", "OM": "Oman", "PK": "Pakistan",
        "PW": "Palau", "PS": "Palestinian Territory, Occupied", "PA": "Panama", "PG": "Papua New Guinea",
        "PY": "Paraguay", "PE": "Peru", "PH": "Philippines", "PN": "Pitcairn", "PL": "Poland", "PT": "Portugal",
        "PR": "Puerto Rico", "QA": "Qatar", "RE": "Réunion", "RO": "Romania", "RU": "Russian Federation",
        "RW": "Rwanda", "BL": "Saint Barthélemy", "SH": "Saint Helena, Ascension and Tristan da Cunha",
        "KN": "Saint Kitts and Nevis", "LC": "Saint Lucia", "MF": "Saint Martin (French part)",
        "PM": "Saint Pierre and Miquelon", "VC": "Saint Vincent and the Grenadines", "WS": "Samoa",
        "SM": "San Marino", "ST": "Sao Tome and Principe", "SA": "Saudi Arabia", "SN": "Senegal", "RS": "Serbia",
        "SC": "Seychelles", "SL": "Sierra Leone", "SG": "Singapore", "SX": "Sint Maarten (Dutch part)",
        "SK": "Slovakia", "SI": "Slovenia", "SB": "Solomon Islands", "SO": "Somalia", "ZA": "South Africa",
        "GS": "South Georgia and the South Sandwich Islands", "ES": "Spain", "LK": "Sri Lanka", "SD": "Sudan",
        "SR": "Suriname", "SS": "South Sudan", "SJ": "Svalbard and Jan Mayen", "SZ": "Swaziland", "SE": "Sweden",
        "CH": "Switzerland", "SY": "Syrian Arab Republic", "TW": "Taiwan, Province of China", "TJ": "Tajikistan",
        "TZ": "Tanzania, United Republic of", "TH": "Thailand", "TL": "Timor-Leste", "TG": "Togo", "TK": "Tokelau",
        "TO": "Tonga", "TT": "Trinidad and Tobago", "TN": "Tunisia", "TR": "Turkey", "TM": "Turkmenistan",
        "TC": "Turks and Caicos Islands", "TV": "Tuvalu", "UG": "Uganda", "UA": "Ukraine",
        "AE": "United Arab Emirates", "GB": "United Kingdom", "US": "United States",
        "UM": "United States Minor Outlying Islands", "UY": "Uruguay", "UZ": "Uzbekistan", "VU": "Vanuatu",
        "VE": "Venezuela, Bolivarian Republic of", "VN": "Viet Nam", "VG": "Virgin Islands, British",
        "VI": "Virgin Islands, U.S.", "WF": "Wallis and Futuna", "YE": "Yemen", "ZM": "Zambia", "ZW": "Zimbabwe"}


def phoneValidation(phno, cntry):
    try:
        c = phone_country(phno)
        if data[c] == cntry:
            return phno
        else:
            return None
    except:
        return None


df_cleaned = df_cleaned.select('Id', 'DisplayName', 'Location', 'Cleaned_AboutMe', 'BirthDate', 'PhoneNo', 'Country', 'State', 'City')
df_validated = df_cleaned.rdd.map(lambda line: Row(Id=line[0],
                                           DisplayName=line[1],
                                           Location=line[2],
                                           AboutMe=line[3],
                                           BirthDate=dateValidation(line[4]),
                                           PhoneNumber=phoneValidation(line[5], line[6]),
                                           Country=line[6],
                                           State=line[7],
                                           City=line[8]))
df_validated = df_validated.cache()
df_validated = sparkSession.createDataFrame(df_validated, samplingRatio=0.2)
df_validated.show()

print("Total rows are : ", df_validated.count())
print("The number of invalid data in each columns are : ")
df_validated.select([count(when(isnull(c), c)).alias(c) for c in df_validated.columns]).show()

# df_validated.write.mode("overwrite").json("file:///home/rahulw/PycharmProjects/Final_Project/Ingestion/dq_good/part3")

df_validated.write.mode("overwrite").json("hdfs://localhost:9000/SparkERIngest/dq_good/part3.json")

# df_validated.write.mode('overwrite').json("/home/riann-rnr/Desktop/PROJECT/Validation/")
sparkSession.catalog.clearCache()
