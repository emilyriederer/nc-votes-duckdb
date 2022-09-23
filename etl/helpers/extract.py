import urllib.request
from zipfile import ZipFile
import os

def extract(url: str, dest: str, target: str = '') -> None:
    """
    Retrieve online data sources from flat or zipped CSV.
    Places data in data/raw subdirectory (first creating, as needed).
    For zip file, automatically unzip target file. 

    Args:
        url (str): URL path to the source file to be downloaded 
        dest (str): File  for the destination file to land
        target (str, optional): Name of file to extract (in case of zipfile). Defaults to ''.
    """

    # set-up expected directory structure, if not exists
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists('data/raw'):
        os.mkdir('data/raw')
    
    # download file to desired location
    dest_path = os.path.join('data', 'raw', dest)
    urllib.request.urlretrieve(url, dest_path)

    # unzip and clean-up (remove zip) if needed
    if target != '':
        with ZipFile(dest_path, 'r') as zip_obj:
            zip_obj.extract(target, path = "data//raw")
        os.remove(dest_path)
