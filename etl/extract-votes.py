from helpers.extract import extract

url_earlyvt = 'https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_11_08/absentee_20221108.zip'
extract(url_earlyvt, 'earlyvt.zip', 'absentee_20221108.csv')
