import time
import uuid
import json
import os
import enum
import boto3
from base64 import b64decode
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.io.twistedreactor import TwistedConnection
from cassandra import ConsistencyLevel
from cassandra.cqlengine import columns, connection, models
from cassandra.cqlengine.management import sync_table


# AWS Lambda let us define some environment variable that we will retrieve.
# ENDPOINT is a comma separated list of Cassandra IP addresses.
ENDPOINT = '54.221.168.218'
LOCAL_DC = os.environ['local_dc']

# cassandra_session is defined outside of Lambda_handler.
# The session will be created at first container invocation
# any subsequent container invocation will be using the same connection.
cassandra_session = None

class Genre(models.Model):
    # Genre Table
    genre_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    name     = columns.Text(required=True)


class Award(models.Model):
    # Award Table
    award_id     = columns.UUID(primary_key=True, default=uuid.uuid4)
    title        = columns.Ascii(max_length=50, required=True)
    organization = columns.Ascii(max_length=100, required=True)


class Actor(models.Model):
    # Actor Class
    actor_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    name     = columns.Ascii(max_length=80, required=True)
    age      = columns.Integer(required=True)
    country  = columns.Set(value_type=str)


class Director(models.Model):
    # Director Class
    director_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    name        = columns.Ascii(max_length=80, required=True)
    age         = columns.Integer(required=True)
    country     = columns.Set(value_type=str)


class Gender(enum.Enum):
    # Gender enum
    Male   = 1
    Female = 2
    Other  = 3

class User(models.Model):
    # User model
    user_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    nick    = columns.Ascii(max_length=25)
    gender  = columns.UserDefinedType(Gender, required=True)




def Lambda_handler(event, context):
    global cassandra_session
    if not cassandra_session:
        cluster = Cluster(ENDPOINT.split(","),connection_class=TwistedConnection, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=LOCAL_DC))
        cassandra_session = cluster.connect()
    print ("initializing the data model")
    cassandra_session.execute("DROP KEYSPACE IF EXISTS ks")
    cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'NetworkTopologyStrategy', '" + LOCAL_DC + "': 3}")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS ks.tb (order_id uuid, name text, address text, phone text, item text, PRIMARY KEY (order_id))")
    return {
        'message': 'ok'
    }
