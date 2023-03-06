#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List 


FILE_PATH = "D:\SSD_DEVICE_STORAGE\Desktop\INeuron\SparkAssignment\SeoulFloating.csv"
columns=['date', 'hour', 'birth_year', 'sex', 'province', 'city', 'fp_num']

API_KEY = 'HZY5P2Q5RYPFI7MN' 
ENDPOINT_SCHEMA_URL  = 'https://psrc-e0919.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'nf2/OD7tH8KmRhDvtm0sF07HW7LaLXkhMUH5ZFizgyDn6v1Bi85DPjwccEPQ6IDe'
BOOTSTRAP_SERVER = 'pkc-3w22w.us-central1.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '3IXYNFOF46XODSSR'
SCHEMA_REGISTRY_API_SECRET = 'n2H8cAPz8xk7zaGhAZlVz3ggDQY/90ubXbz6FofjRWWNqQAt7PZHJfuMNIL1BMjM'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY,
                'queue.buffering.max.messages': 1100000
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def get_car_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,0:]
    df=df.fillna("NAN")
    
    cars:List[Car]=[]
    for data in df.values:
        car=Car(dict(zip(columns,data)))
        cars.append(car)
        yield car

def car_to_dict(car:Car, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return car.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
  "date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "hour": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "birth_year": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "sex": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "province": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "city": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "fp_num": {
      "description": "The type(v) type is used.",
      "type": "number"
      }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    # schema_registry_conf = schema_config()
    # schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # #reading the latest version of schema and schema_str from schema registry and useing it for data serialization.
    # schema_name = schema_registry_client.get_schema(100002).schema_str

    # string_serializer = StringSerializer('utf_8')
    # json_serializer = JSONSerializer(schema_name, schema_registry_client, car_to_dict)

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for idx,car in enumerate(get_car_instance(file_path=FILE_PATH)):

            print(car)
            producer.produce(topic=topic, # publishing data in Kafka Topic one by one and use static key
                            key=string_serializer(str(uuid4()), car_to_dict),
                            value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
           
            
            # if idx==10:    # index value is used for testing 1 or 2 records            
            #     break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("SeoulFloating")