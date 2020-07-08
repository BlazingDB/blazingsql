import json
import sys


with open(sys.argv[1], 'r') as f:
    conf = json.load(f)

print(conf['TestSettings']['dataDirectory'])
