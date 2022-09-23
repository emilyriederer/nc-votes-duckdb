from helpers.extract import extract

url_history = 'https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip'
extract(url_history, 'history.zip', 'ncvhis_Statewide.txt')
