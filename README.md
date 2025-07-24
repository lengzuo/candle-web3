# Real-Time Candle Service

This project implements a real-time data processing system as described in the test assignment. It consists of multiple services that connect to cryptocurrency exchanges, aggregate trade data, build OHLC candles, and stream them to clients in real-time.

## Architecture

The system is designed with a microservices architecture, where each component is a separate service that can be scaled independently. The services communicate asynchronously via a Kafka message broker.

- **Connectors (`binance-connector`, `kraken-connector`, `coinbase-connector`):** These services connect to the WebSocket endpoints of their respective exchanges, subscribe to trade streams for a predefined list of pairs (BTC-USDT, ETH-USDT, SOL-USDT), and publish the raw trade data to a Kafka topic.

- **Kafka:** Acts as the central message bus, decoupling the data producers (connectors) from the data consumer (candles-service). This provides resilience and scalability.

- **Redis Pub/Sub:** Used by the `candles-service` for internal broadcasting of finalized candles. The `Publisher` component sends candles to Redis channels, and the `Broadcaster` component subscribes to these channels to fan out data to connected gRPC clients. This allows for efficient, low-latency distribution of candles within the `candles-service` or across multiple instances if scaled.

- **`candles-service`:** This service consumes the raw trade data from Kafka. It contains an `Aggregator` that consolidates trades from all exchanges and builds OHLC candles for each instrument pair at a configurable interval. The finalized candles are then broadcasted to connected clients via a gRPC server.

  - It uses a `Publisher` to send candles to Redis Pub/Sub and a `Broadcaster` to receive candles from Redis Pub/Sub and distribute them to gRPC clients.

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

- **Go:** Chosen for its strong performance, excellent concurrency support, and robust standard library, making it well-suited for high-throughput data processing applications.

- **Docker & Docker Compose:** Used to create a consistent, reproducible environment for development and deployment. This simplifies setup and ensures that all services run correctly together.

- **Kafka:** Selected as the messaging backbone to decouple services and provide a scalable, fault-tolerant way to handle high volumes of trade data. This allows for easy addition of new data sources or consumers in the future.

- **gRPC:** Chosen for real-time communication between the `candles-service` and its clients. gRPC is highly efficient, supports streaming, and provides a strongly-typed contract-first approach to API design with Protocol Buffers.

- **Redis Pub/Sub:** Utilized for efficient, in-memory message broadcasting within the `candles-service`. It provides a simple and fast mechanism for the `Publisher` to send candles and the `Broadcaster` to receive them, enabling fan-out to multiple gRPC clients.

- **Zerolog:** Used for structured logging, which is essential for monitoring and debugging in a distributed system.

## Future Improvements and Considerations

For real life applications, the following are areas that could be enhanced in a production environment:

- **Dynamic Client Subscriptions:** The gRPC `subscribe` endpoint could be enhanced to allow clients to modify their subscriptions at any time without reconnecting.

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
