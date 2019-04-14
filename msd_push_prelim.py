import os
import hdf5_getters
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# SCRIPT PARAMETERS
DATA_DIR = "data"

CLUSTER_IP = ['127.0.0.1']
REPLICATION_FACTOR = 1

KEYSPACE = "music"
TABLENAME = 'msd_data'

FIELDS = [
    'artist_familiarity',
    'artist_hotttnesss',
    'artist_id',
    'artist_mbid',
    'artist_playmeid',
    'artist_7digitalid',
    'artist_latitude',
    'artist_longitude',
    'artist_location',
    'artist_name',
    'release',
    'release_7digitalid',
    'song_id',
    'song_hotttnesss',
    'title',
    'track_7digitalid',
    'similar_artists',
    'analysis_sample_rate',
    'audio_md5',
    'danceability',
    'duration',
    'end_of_fade_in',
    'energy',
    'key',
    'key_confidence',
    'loudness',
    'mode',
    'mode_confidence',
    'start_of_fade_out',
    'tempo',
    'time_signature',
    'time_signature_confidence',
    'track_id',
    'year'
]


# HELPER FUNCTIONS
def get_data_from_file(filepath):

    # TODO: Make sure number of songs in a h5 file is ONE
    # TODO: OR change function to parse multiple rows

    h5 = hdf5_getters.open_h5_file_read(filepath)
    try:
        data = dict()
        for field in FIELDS:
            result = getattr(hdf5_getters, 'get_{}'.format(field))(h5)
            data[field] = result

        return data
    finally:
        h5.close()


# INIT
cluster = Cluster(CLUSTER_IP)
session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS {} "
                "WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}"
                .format(KEYSPACE, REPLICATION_FACTOR))

session.execute("USE {}".format(KEYSPACE))

session.execute("CREATE TABLE IF NOT EXISTS {} ( "
                "track_id text PRIMARY KEY, "
                "artist_familiarity double, "
                "artist_hotttnesss double, "
                "artist_id text, "
                "artist_mbid text, "
                "artist_playmeid int, "
                "artist_7digitalid int, "
                "artist_latitude double, "
                "artist_longitude double, "
                "artist_location text, "
                "artist_name text, "
                "release text, "
                "release_7digitalid int, "
                "song_id text, "
                "song_hotttnesss double, "
                "title text, "
                "track_7digitalid int, "
                "similar_artists list<text>, "
                "analysis_sample_rate int, "
                "audio_md5 text, "
                "danceability double, "
                "duration double, "
                "end_of_fade_in double, "
                "energy double, "
                "key int, "
                "key_confidence double, "
                "loudness double, "
                "mode int, "
                "mode_confidence double, "
                "start_of_fade_out double, "
                "tempo double, "
                "time_signature int, "
                "time_signature_confidence double, "
                "year int"
                ")".format(TABLENAME))


query = SimpleStatement("INSERT INTO {}.{} ("
                        "track_id, artist_familiarity, artist_hotttnesss, artist_id, artist_mbid, artist_playmeid, "
                        "artist_7digitalid, artist_latitude, artist_longitude, artist_location, artist_name, release, "
                        "release_7digitalid, song_id, song_hotttnesss, title, track_7digitalid, similar_artists, "
                        "analysis_sample_rate, audio_md5, danceability, duration, end_of_fade_in, energy, key, "
                        "key_confidence, loudness, mode, mode_confidence, start_of_fade_out, tempo, time_signature, "
                        "time_signature_confidence, year) "
                        "VALUES ("
                        "%(track_id)s, %(artist_familiarity)s, %(artist_hotttnesss)s, %(artist_id)s, %(artist_mbid)s, "
                        "%(artist_playmeid)s, %(artist_7digitalid)s, %(artist_latitude)s, %(artist_longitude)s, "
                        "%(artist_location)s, %(artist_name)s, %(release)s, %(release_7digitalid)s, %(song_id)s, "
                        "%(song_hotttnesss)s, %(title)s, %(track_7digitalid)s, %(similar_artists)s, "
                        "%(analysis_sample_rate)s, %(audio_md5)s, %(danceability)s, %(duration)s, %(end_of_fade_in)s, "
                        "%(energy)s, %(key)s, %(key_confidence)s, %(loudness)s, %(mode)s, %(mode_confidence)s, "
                        "%(start_of_fade_out)s, %(tempo)s, %(time_signature)s, %(time_signature_confidence)s, %(year)s)"
                        .format(KEYSPACE, TABLENAME))


# READ DIR AND PUSH TO DB
for dir, subdirs, files in os.walk(DATA_DIR):
    for file in files:
        filepath = os.path.join(dir, file)
        data = get_data_from_file(filepath)

        # Insert data into table
        session.execute(query, data)


print("Wohoo!")
