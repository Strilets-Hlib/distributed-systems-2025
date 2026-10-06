# Distributed Systems Microservices

**Author:** Hlib Strilets


This project demonstrates a microservices system for submitting, retrieving, and storing messages. Clients send HTTP requests to `facade-service`, which communicates with `messages-service` and `logging-service`.

Depending on the configuration, service addresses are provided by `server-config` or discovered through Consul. `logging-service` can store messages in a Hazelcast Distributed Map, while Kafka can be used as a message queue for delivering messages to `messages-service` instances.

## Components

- **facade-service** — HTTP API for clients; accepts `GET` and `POST` requests.
- **messages-service** — processes messages received by the system.
- **logging-service** — stores messages; the Hazelcast configuration uses a Distributed Map.
- **server-config** — provides the facade with service addresses.
- **Hazelcast** — distributed storage for message records.
- **Kafka** — message queue.
- **Consul** — service discovery and configuration storage.

## Running the services

The project is built with Rust and run with Cargo. There is a signle python script. The requirements for the conda environment are provided alongside the script. Run commands from the repository root. For configurations that use Hazelcast, Kafka, or Consul, start the relevant infrastructure and configure its addresses and settings first.

Example: start three `logging-service` instances connected to Hazelcast:

```bash
cargo run --release -p logging-service -- \
  --hazelcast-map stor -p 9898 --hazelcast-port 5701 --cluster-name lab
```

```bash
cargo run --release -p logging-service -- \
  --hazelcast-map stor -p 9899 --hazelcast-port 5702 --cluster-name lab
```

```bash
cargo run --release -p logging-service -- \
  --hazelcast-map stor -p 9900 --hazelcast-port 5703 --cluster-name lab
```

Start a `messages-service` instance:

```bash
cargo run --release -p messages-service -- -p 7878
```

If the configuration uses `server-config`, start it with the service addresses:

```bash
cargo run --release -p server-config -- \
  -p 5750 \
  --messages 127.0.0.1:7878 \
  --logging-list 127.0.0.1:9898 127.0.0.1:9899 127.0.0.1:9900
```

Then start the facade:

```bash
cargo run --release -p facade-service -- \
  --port 8362 \
  --server-config 127.0.0.1:5750 \
  --mesage-service 127.0.0.1:7878
```

The required components and command-line arguments depend on the selected configuration. For example, the Kafka setup requires a running Kafka cluster, a created topic, and the corresponding `--kafka-topic` and `--consume-from` arguments. The Consul setup requires a running Consul agent and configuration entries in its KV store. Different setups are currenctly on different branches.

## Using the API

Submit a message with an HTTP `POST` request:

```bash
curl -i -X POST http://127.0.0.1:8362 -d "Hello, world!"
```

Retrieve a message with an HTTP `GET` request:

```bash
curl -i -X GET http://127.0.0.1:8362
```

Submit several messages:

```bash
for i in $(seq 1 5); do
  curl -i -X POST http://127.0.0.1:8362 -d "msg$i"
done
```

In the basic configuration, the logging service stores each message and its UUID in a `HashMap`. In the Hazelcast configuration, records are stored in a Distributed Map. In the Kafka configuration, the facade sends messages to the queue, and `GET` retrieves a message through an available `messages-service` instance.

## Troubleshooting and behavior

If required services are unavailable, a request may fail with `500 Internal Server Error`. If the cluster or queue is empty, `GET` may return an empty response. During testing, forcibly shutting down Kafka nodes caused some messages to be lost; operations also failed when the Kafka coordinator was unavailable.

Some services support the `-d` option for diagnostic output. In the Consul configuration, unavailable services are marked as `critical` and are not treated as available instances.