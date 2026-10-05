
import hazelcast

import base64
import argparse
import json


from hazelcast.serialization.api import IdentifiedDataSerializable

class Base64Serializer(IdentifiedDataSerializable):
    def __init__(self, value=None, contentType = None):
        self.value = value
        self.contentType = contentType

    def get_class_id(self):
        return 1

    def get_factory_id(self):
        return -25

    def write_data(self, output):
        output.write_string(base64.b64encode(self.value))
        output.contentType(base64.b64encode(self.contentType))

    def read_data(self, input):
        self.value = base64.b64decode(input.read_string())
        self.contentType = base64.b64decode(input.read_string())

class MyData(IdentifiedDataSerializable):
    def __init__(self, value = None):
        self.value = value

    def write_data(self, output):
        output.write_string(self.value)
        #output.contentType(self.contentType)

    def read_data(self, input):
        self.value = input.read_string()
        #self.contentType = input.read_string()

    def get_class_id(self):
        return 1

    def get_factory_id(self):
        return -25

factory = {
    1: MyData
}

# Defining main function
def main():
    
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("--ip", required=True, help="IP address of the Hazelcast member")
    parser.add_argument("--cluster_name", required=True, help="Name of the Hazelcast cluster")
    parser.add_argument("--map_name", required=True, help="Name of the map to retrieve data from")
    parser.add_argument("-d", "--debug", help="Show debug data", action="store_true")
    args = parser.parse_args()


    client = hazelcast.HazelcastClient(
        cluster_name=args.cluster_name,
        cluster_members=[str(args.ip)],
        data_serializable_factories={
            -25: factory
        }
    )
    try:


        if args.debug:
            print(f"Connecting to cluster '{args.cluster_name}' at {args.ip}...")
        
        # Get the map
        my_map = client.get_map(args.map_name).blocking()

        if args.debug:
            print("Keys in the map:")
        res = [key for key in my_map.key_set()]

        res = [val.value for val in my_map.values()]
        print(res)
        return res

    except hazelcast.errors.HazelcastSerializationError as e:
        print(f"Serialization error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Shutdown the client
        client.shutdown()
    return
if __name__=="__main__":
    main()