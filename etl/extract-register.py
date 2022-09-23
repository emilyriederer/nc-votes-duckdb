from helpers.extract import extract

url_vtroles = 'https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip'
extract(url_vtroles, 'vtroles.zip', 'ncvoter_Statewide.txt')