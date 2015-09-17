from client import Blueflood, Datapoint
from utils import time_in_ms
import pprint

auth_url = 'https://identity.api.rackspacecloud.com/v2.0/'
apikey = '0e688a460988337e0e759524a2ccfc33'
username = 'privateclouddevs'

client = Blueflood(auth_url=auth_url, apikey=apikey, username="privateclouddevs")


point = {
    'collectionTime': 1442262994835,
    'metricName': 'james.test.number',
    'metricValue': 55,
    'ttlInSeconds': 3600
}

#point = Datapoint('intel.suda-devstack-dfw.Threads_created', 55, collection_time=1442262994835, ttl_seconds=3600)
print "Ingesting"
client.ingest(point)

print "Getting"
resp = client.get_metrics(0, time_in_ms(), ['james.test.number'], points=100)
pprint.pprint(resp)

