# Real-Time Candle Service

This project implements a real-time data processing system as described in the test assignment. It consists of multiple services that connect to cryptocurrency exchanges, aggregate trade data, build OHLC candles, and stream them to clients in real-time.

## Architecture

The system is designed with a microservices architecture, where each component is a separate service that can be scaled independently. The services communicate asynchronously via a Kafka message broker.

- **Connectors (`binance-connector`, `kraken-connector`, `coinbase-connector`):** These services connect to the WebSocket endpoints of their respective exchanges, subscribe to trade streams for a predefined list of pairs (BTC-USDT, ETH-USDT, SOL-USDT), and publish the raw trade data to a Kafka topic.

- **Kafka:** Acts as the central message bus, decoupling the data producers (connectors) from the data consumer (candles-service). This provides resilience and scalability.

- **`candles-service`:** This is the core service that consumes raw trades from Kafka. It contains an `Aggregator` to build OHLC candles. To enable horizontal scaling, the service's internal components are decoupled using Redis Pub/Sub for broadcasting finalized candles.

- **Redis Pub/Sub:** While Kafka handles data ingress, Redis is used for high-speed, low-latency broadcasting of the _finalized_ candles to all connected gRPC clients. This decouples the candle aggregation logic from the client-facing broadcast logic. Crucially, it allows the `candles-service` to be scaled horizontally: multiple instances can all publish to and subscribe from the same Redis channels, ensuring all clients receive all candles regardless of which instance they are connected to.

- **`client-service`:** A simple grpc client that connects to the `candles-service`, subscribes to a list of instrument pairs, and prints the received candle data to standard output.

## How to Build and Run

The entire system is containerized using Docker and can be orchestrated with Docker Compose.

### Prerequisites

- Docker
- Docker Compose

### Running the System

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Build and start all services:**
    ```bash
    docker-compose up --build
    ```

This command will build the Docker images for each service and start them in the correct order. The `client-service` will start printing candle data to the console once the `candles-service` is ready.

## Configuration

- **Candle Interval:** The OHLC candle interval in `candles-service` can be configured via the `--interval` command-line flag. The default value is 5 seconds.

  _Example in `docker-compose.yml`:_

  ```yaml
  # In candler-service entrypoint
  entrypoint: ./candles-service --interval=10s
  ```

- **Client Subscriptions:** The instrument pairs for the `client-service` to subscribe to can be configured via the `--pairs` command-line flag.

  _Example in `docker-compose.yml`:_

  ```yaml
  # In client-service entrypoint
  entrypoint: ./client-service --server=candles-service:8080 --pairs=btcusdt,ethusdt,solusdt
  ```

## Technical Decisions

- **Docker & Docker Compose:** Used to create a consistent, reproducible environment for development and deployment. This simplifies setup and ensures that all services run correctly together.

- **Kafka:** Selected as the messaging backbone to decouple services and provide a scalable, fault-tolerant way to handle high volumes of trade data. This allows for easy addition of new data sources or consumers in the future.

- **gRPC:** Chosen for real-time communication between the `candles-service` and its clients. gRPC is highly efficient, supports streaming, and provides a strongly-typed contract-first approach to API design with Protocol Buffers.

- **Redis Pub/Sub:** Utilized for efficient, in-memory message broadcasting within the `candles-service`. It provides a simple and fast mechanism for the `Publisher` to send candles and the `Broadcaster` to receive them, enabling fan-out to multiple gRPC clients.

- **Zerolog:** Used for structured logging, which is essential for monitoring and debugging in a distributed system.

## Future Improvements and Considerations

For real life applications, the following are areas that could be enhanced in a production environment:

- **Error Handling and Resilience:**

  - Implement more sophisticated error handling in the connectors, including exponential backoff and retry mechanisms for WebSocket connections.
  - Add dead-letter queues in Kafka to handle malformed or unprocessable messages.

- **State Persistence:** The `candles-service` is currently stateless. For improved fault tolerance, the state of active candles could be periodically snapshotted to a persistent store like Redis or a timeseries database. This would allow the service to recover its state after a restart.

- **Scalability:**

  - The `candles-service` is a single point of failure. It could be scaled horizontally by partitioning the instrument pairs across multiple instances.
  - Kafka topics can be partitioned to increase throughput.

- **Monitoring and Alerting:**

  - Integrate with a monitoring system like Prometheus and Grafana. Expose metrics from each service (e.g., number of trades processed, active client connections, candle generation latency).
  - Set up alerting for critical events, such as a connector disconnecting or a service becoming unhealthy.

- **Testing:**

  - Add more comprehensive integration and end-to-end tests to validate the entire data pipeline.
  - Increase unit test coverage across all services.

- **Security:**
  - Secure the gRPC endpoint with TLS and authentication/authorization mechanisms if exposed externally.
