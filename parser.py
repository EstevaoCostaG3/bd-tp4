"""
Parser for Amazon product co-purchasing network metadata
 
https://snap.stanford.edu/data/amazon-meta.html
"""

import re
import gzip
from tqdm import tqdm

def parse(filename, total):
  IGNORE_FIELDS = ['Total items', 'reviews']
  f = gzip.open(filename, 'r')
  entry = {}
  categories = []
  reviews = []
  similar_items = []
  
  for line in tqdm(f, total=total):
    line = line.strip()
    colonPos = line.find(b':')

    if line.startswith(b"Id"):
      if reviews:
        entry["reviews"] = reviews
      if categories:
        entry["categories"] = categories
      yield entry
      entry = {}
      categories = []
      reviews = []
      rest = line[colonPos+2:]
      entry["id"] = str(rest.strip(), errors='ignore')
      
    elif line.startswith(b"similar"):
      similar_items = line.split()[2:]
      entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
    elif line.find(b"cutomer:") != -1:
      review_info = line.split()

      reviews.append({'_date': review_info[0],
                      'customer_id': review_info[2], 
                      'rating': int(review_info[4]), 
                      'votes': int(review_info[6]), 
                      'helpful': int(review_info[8])})

    elif line.startswith(b"|"):
      categories.append(line)

    elif colonPos != -1:
      eName = line[:colonPos]
      rest = line[colonPos+2:]

      if not eName in IGNORE_FIELDS:
        entry[eName] = str(rest.strip(), errors='ignore')

  if reviews:
    entry["reviews"] = reviews
  if categories:
    entry["categories"] = categories
    
  yield entry


if __name__ == '__main__':
  file_path = "../amazon-meta.txt.gz"

  import simplejson

  for e in parse(file_path, 15010574):
    if e:
      print(simplejson.dumps(e))
