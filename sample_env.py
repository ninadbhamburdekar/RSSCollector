import os
import ssl
from elasticsearch import Elasticsearch

### EDIT AND SAVE THIS FILE AS env.py

## For handling ssl issues
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

## Define Elasticsearch nodes and creds
ESNODES = [
    "https://elasticsearch:9201",
           ]
ELASTICUSER = "your_elasticsearch_username"
ELASTICPASSWORD = "<PASSWORD>"

## Pass Elasticsearch creds and create client
def setup_esclient(ESNODES):
     esclient = Elasticsearch(
            ESNODES,
            basic_auth=(ELASTICUSER, ELASTICPASSWORD),
            verify_certs=False
        )
     return esclient

## X API Token. Define as an ENV variable OPENAIAPI_TOKEN else value defined below will be used.
## Example - Define by running in shell: export OPENAIAPI_TOKEN=AAAAA...
openai_api_token = os.environ.get('OPENAIAPI_TOKEN',
                                   'YOUR_TOKEN')


