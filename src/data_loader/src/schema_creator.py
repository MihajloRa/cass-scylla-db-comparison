from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Define the CQL statements for table creation
table_creation_statements = [
    """
    CREATE TABLE IF NOT EXISTS measurement_info (
        station_code int,
        measurement_date date,
        measurement_time time,
        item_code int,
        average_value double,
        instrument_status int,
        PRIMARY KEY ((station_code, measurement_date), measurement_time, item_code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS measurement_item_info (
        item_code int PRIMARY KEY,
        item_name text,
        unit_of_measurement text,
        good double,
        normal double,
        bad double,
        very_bad double
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS measurement_station_info (
        station_code int PRIMARY KEY,
        station_name text,
        address text,
        latitude double,
        longitude double
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS measurement_summary (
        station_code int,
        measurement_date date,
        measurement_time time,
        address text,
        latitude double,
        longitude double,
        so2 double,
        no2 double,
        o3 double,
        co double,
        pm10 double,
        pm25 double,
        PRIMARY KEY ((station_code, measurement_date), measurement_time)
    );
    """
]

def create_tables(session):
    for statement in table_creation_statements:
        session.execute(statement)

def create_session(node_ips, keyspace_name):
    cluster = Cluster(node_ips)
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
        AND durable_writes = true;
    """)
    session.set_keyspace(keyspace_name)
    return session

if __name__ == "__main__":
    node_ips = ['localhost:']  # IP addresses of your Cassandra/Scylla nodes
    keyspace_name = 'air_pollution_seoul'
    
    session = create_session(node_ips, keyspace_name)
    create_tables(session)
    print("Keyspace and tables created successfully.")