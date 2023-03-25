import os
import requests
import tarfile
from tqdm import tqdm

url = "https://pub-3916d8c82abb435eb70175747fdc2119.r2.dev/datasets.tar.gz"
filename = "datasets.tar.gz"

# Check if the file already exists
# Download the file
response = requests.get(url, stream=True)
total_size = int(response.headers.get("content-length", 0))
block_size = 1024
progress_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
with open(filename, "wb") as f:
    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        f.write(data)
progress_bar.close()

# Extract the contents of the file
with tarfile.open(filename, "r:gz") as tar:
    tar.extractall(path='./')
