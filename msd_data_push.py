import os
import sys
import csv
import random
import logging
import argparse
import hdf5_getters
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# ------------------------------
# SCRIPT CONFIG
KEYSPACE = "music"
TABLENAME = 'msd'

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

CSV_FIELDS = [
    'csv_id',
    'title',
    'artist_name',
    'release',
    'year'
]

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


# ------------------------------
# RANDOM DATA GENERATION

def randint():
    return random.randint(0, 10)


def randdouble():
    return random.random() * 10


def randtext():
    return "RANDOMID" + str(random.randrange(10000000, 99999999))


def randlist():
    return list()


FIELD_VAL = {
    'artist_familiarity': randdouble,
    'artist_hotttnesss': randdouble,
    'artist_id': randtext,
    'artist_mbid': randtext,
    'artist_playmeid': randint,
    'artist_7digitalid': randint,
    'artist_latitude': randdouble,
    'artist_longitude': randdouble,
    'artist_location': randtext,
    'release_7digitalid': randint,
    'song_id': randtext,
    'song_hotttnesss': randdouble,
    'track_7digitalid': randint,
    'similar_artists': randlist,
    'analysis_sample_rate': randint,
    'audio_md5': randtext,
    'danceability': randdouble,
    'duration': randdouble,
    'end_of_fade_in': randdouble,
    'energy': randdouble,
    'key': randint,
    'key_confidence': randdouble,
    'loudness': randdouble,
    'mode': randint,
    'mode_confidence': randdouble,
    'start_of_fade_out': randdouble,
    'tempo': randdouble,
    'time_signature': randint,
    'time_signature_confidence': randdouble,
}


# ------------------------------
# METHODS

def validate_db(session):
    # keyspace exists?
    if len(session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{}'".format(KEYSPACE))
                   .current_rows) > 0:
        logging.info("Verified keyspace existence for '{}'.".format(KEYSPACE))
    else:
        logging.error("Keyspace '{}' does not exist. Run the schema setup first.".format(KEYSPACE))
        raise RuntimeError()

    # table exists?
    if len(session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'"
                                   .format(KEYSPACE, TABLENAME)).current_rows) > 0:
        logging.info("Verified table existence for '{}'.".format(TABLENAME))
    else:
        logging.error("Table '{}' does not exist. Run the schema setup first.".format(TABLENAME))
        raise RuntimeError()

    return


def parse_args(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    req = parser.add_argument_group('required arguments')
    req.add_argument("-c", "--cluster", help="one or more cluster ip addresses", nargs="+", type=str)
    parser.add_argument("-d", "--directory", help="data directory location", nargs=1, type=str)
    parser.add_argument("-f", "--csv-file", help="csv file path", nargs=1, type=str)

    args = parser.parse_args(argv)

    if args.directory and args.cluster:
        return args.directory, args.cluster, True

    if args.csv_file and args.cluster:
        return args.csv_file[0], args.cluster, False

    parser.print_help()
    print("You must provide either the directory or the csv-file.")
    exit(2)


def get_data_from_file(filepath):
    h5 = hdf5_getters.open_h5_file_read(filepath)
    try:
        data = dict()
        for field in FIELDS:
            result = getattr(hdf5_getters, 'get_{}'.format(field))(h5)
            data[field] = result

        return data
    finally:
        h5.close()


def _connect_cassandra(cluster_ips):
    try:
        cluster = Cluster(cluster_ips)
        session = cluster.connect()
    except Exception as e:
        logging.exception(e)
        raise
    else:
        logging.info("Connected to Cassandra cluster {}".format(cluster_ips))
        return session


def push_from_dir(data_dir, session):

    assert os.path.isdir(data_dir)

    for dir, subdirs, files in os.walk(data_dir[0]):
        for file in files:
            filepath = os.path.join(dir, file)
            msd_data = get_data_from_file(filepath)

            # Insert data into table
            session.execute(query, msd_data)

            msd_data.pop('similar_artists')
            logging.debug(msd_data)

        logging.info("Pushed data from '{}'".format(os.path.abspath(dir)))


def push_from_csv(csv_filepath, session):

    assert os.path.isfile(csv_filepath)

    with open(csv_filepath) as csv_file:
        csv_reader = csv.DictReader(csv_file)

        try:
            assert csv_reader.fieldnames == CSV_FIELDS
        except AssertionError:
            logging.error("Incompatible csv. Check the fieldnames in the csv file.")
            raise

        for song in csv_reader:

            msd_data = dict()
            for field in FIELDS:

                # Generate unique track id based on id from csv
                if field == 'track_id':
                    msd_data[field] = "CSVDATA{:04d}".format(int(song['csv_id']))
                    continue

                # Include fields provided by csv file
                if field in CSV_FIELDS:
                    if field == 'year':
                        msd_data[field] = int(song[field])
                    else:
                        msd_data[field] = song[field]
                    continue

                # Randomly generate field values for other fields
                msd_data[field] = FIELD_VAL[field]()

            # Insert data into table
            session.execute(query, msd_data)

            msd_data.pop('similar_artists')
            logging.debug(msd_data)

        logging.info("Pushed data from '{}'".format(csv_filepath))


def push_msd_data():
    try:
        data_location, cluster_ips, is_real_data = parse_args()
        session = _connect_cassandra(cluster_ips)

        validate_db(session)

        if is_real_data:
            push_from_dir(data_location, session)
        else:
            push_from_csv(data_location, session)

    except Exception as e:
        logging.exception(e)
        logging.error("Script terminated unsucessfully.")
    else:
        logging.info("Script terminated successfully.")


# ------------------------------
# DRIVER
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=(
        logging.FileHandler("msd_push_log.txt", mode="w"), logging.StreamHandler(sys.stdout)))

    push_msd_data()
