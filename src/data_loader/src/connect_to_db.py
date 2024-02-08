from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra.auth import PlainTextAuthProvider

def connect_to_db():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
    session = cluster.connect('air_quality')
    return session