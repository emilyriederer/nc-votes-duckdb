from helpers.extract import extract

url_cps_suppl = 'https://www2.census.gov/programs-surveys/cps/datasets/2020/supp/nov20pub.csv'
extract(url_cps_suppl, 'cps_suppl.csv')
