'''
Citation:

MIT Election Data and Science Lab, 2018, "County Presidential Election Returns 2000-2020", 
https://doi.org/10.7910/DVN/VOQCHQ, Harvard Dataverse, V10; countypres_2000-2020.tab [fileName], 
UNF:6:pVAMya52q7VM1Pl7EZMW0Q== [fileUNF] 
'''

from helpers.extract import extract

extract('https://dataverse.harvard.edu/api/access/datafile/6104822', 'data/raw/results_county.csv')
