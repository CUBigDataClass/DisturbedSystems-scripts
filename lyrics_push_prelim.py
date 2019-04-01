"""
Cassandra driver for python is supported in python3 versions 3.3 and 3.4.
Installation guide: https://datastax.github.io/python-driver/installation.html
"""
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# SCRIPT PARAMETERS
FILEPATH = "mxm_dataset_test.txt"

CLUSTER_IP = ['127.0.0.1']
REPLICATION_FACTOR = 1

KEYSPACE = "music"
LYRICS_TABLENAME = 'mxm_lyrics'


# HELPER FUNCTIONS
def parse_headers(line):
    return {i+1: word for i, word in enumerate(line[1:].split(","))}


def parse_data(line):
    return line.split()


# INIT
cluster = Cluster(CLUSTER_IP)
session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS {} "
                "WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}"
                .format(KEYSPACE, REPLICATION_FACTOR))

session.execute("USE {}".format(KEYSPACE))
session.execute("CREATE TABLE IF NOT EXISTS {} ( "
                "track_id text PRIMARY KEY, "
                "mxm_track_id text, "
                "counts map<text, int> "
                ")".format(LYRICS_TABLENAME))

query = SimpleStatement("INSERT INTO {}.{} (track_id, mxm_track_id, counts) "
                        "VALUES (%(track_id)s, %(mxm_track_id)s, %(counts)s) "
                        .format(KEYSPACE, LYRICS_TABLENAME))


# READ FILE AND PUSH TO DB

# Break counter
count = 10

with open(FILEPATH) as mxm_file:
    for line in mxm_file:

        # Comments
        if line.startswith("#"):
            continue

        # Header line. Occurs once
        if line.startswith("%"):
            headers = parse_headers(line)
            continue

        # Data parse
        data = dict()
        bag = line.strip("\n").split(",")
        data['track_id'] = bag[0]
        data['mxm_track_id'] = bag[1]
        data['counts'] = dict()
        for word_count in bag[2:]:
            split = word_count.split(":")
            data['counts'][headers[int(split[0])]] = int(split[1])

        # Insert data into table
        session.execute(query, data)

        # Break counter
        count -= 1
        if count <= 0:
            break


print("Wohoo")
