import argparse
import random
import sys
import time
from uuid import uuid4

from confluent_kafka import Producer
from faker import Faker

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SYSTEM_LOGS
from model import LogLevel, SystemLog

fake = Faker()

SERVICES: list[str] = [
    "payment-gateway",
    "user-auth",
    "inventory-service",
    "frontend-bff",
]

# Weighted distribution: 70% INFO, 20% WARN, 10% ERROR
WEIGHTED_LEVELS: list[LogLevel] = [LogLevel.INFO] * 7 + [LogLevel.WARN] * 2 + [LogLevel.ERROR]


def delivery_report(err, msg) -> None:
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"Delivery error: {err}")
    else:
        print(f"Log sent to {msg.topic()} [{msg.partition()}]")


def _create_message(service: str, level: LogLevel) -> str:
    """Generate a realistic service-specific log message."""
    user_id = f"usr_{fake.numerify('######')}"
    order_id = f"ord_{fake.numerify('########')}"
    sku = f"SKU-{fake.bothify('???-####').upper()}"
    ip = fake.ipv4()

    if service == "user-auth":
        if level == LogLevel.INFO:
            return random.choice([
                f"User authentication successful for {user_id} via oauth2-github",
                f"JWT session token issued for {user_id} with TTL 3600s",
                f"Password reset email dispatched to user profile {user_id}",
                f"Multi-factor challenge completed for {user_id}",
            ])
        if level == LogLevel.WARN:
            return random.choice([
                f"Repeated failed login attempt (3/5) for {user_id} from IP {ip}",
                "Token validation latency exceeded 350ms for issuer auth.corp.internal",
                f"Session expiring within 60s for user {user_id} - refresh requested",
                "Rate limit threshold (80%) reached for endpoint /oauth/token",
            ])
        return random.choice([
            "Authentication provider LDAP timeout contacting ldap.corp.internal:636",
            f"Invalid cryptographic signature on refresh token for user {user_id}",
            "Database connection pool exhausted during credential lookup: HikariPool-1",
            "OAuth2 provider callback failed with HTTP 503 Service Unavailable",
        ])

    if service == "payment-gateway":
        if level == LogLevel.INFO:
            amount = f"{random.uniform(10.0, 500.0):.2f}"
            return random.choice([
                f"Payment authorization approved for {order_id} - amount: ${amount}",
                f"Refund processed successfully for transaction {uuid4()} ({order_id})",
                "Payment webhook delivered to merchant endpoint with HTTP 200",
            ])
        if level == LogLevel.WARN:
            return random.choice([
                "Payment gateway latency warning: Stripe API responded in 1840ms",
                f"Card CVV verification retry required for order {order_id}",
                "High retry count (2/3) encountered for payment settlement batch",
            ])
        return random.choice([
            f"Charge capture declined: insufficient funds for order {order_id}",
            "Stripe API unreachable after 3 exponential backoff retries (HTTP 502)",
            f"Fatal deadlock on transaction ledger update for order {order_id}",
        ])

    if service == "inventory-service":
        if level == LogLevel.INFO:
            qty = random.randint(1, 10)
            return random.choice([
                f"Stock reservation confirmed for {sku} (qty: {qty})",
                "Product catalog catalog-v2 reindexed with 150 updated items",
            ])
        if level == LogLevel.WARN:
            rem = random.randint(1, 5)
            return random.choice([
                f"Low stock warning: {sku} remaining units: {rem}",
                "Catalog cache miss ratio elevated to 42%",
            ])
        return random.choice([
            f"Optimistic lock exception on stock decrement for {sku}",
            f"Database constraint violation: negative stock prevented for {sku}",
        ])

    if service == "frontend-bff":
        if level == LogLevel.INFO:
            latency = random.randint(15, 80)
            return random.choice([
                f"HTTP GET /api/v1/catalog - 200 OK (latency: {latency}ms)",
                "GraphQL query /dashboard executed successfully in 45ms",
            ])
        if level == LogLevel.WARN:
            return random.choice([
                "Client request timeout after 5000ms on POST /api/v1/checkout",
                "Circuit breaker half-open probe dispatched to payment-gateway",
            ])
        return random.choice([
            "HTTP 500 Internal Server Error returned: upstream payment-gateway timeout",
            "Circuit breaker OPEN for inventory-service on endpoint /api/v1/cart",
        ])

    return f"{level.value} log event from {service}"


def generate_mock_log(
    service_name: str | None = None,
    level: LogLevel | None = None,
) -> SystemLog:
    """Generate a realistic synthetic SystemLog."""
    resolved_service = service_name if service_name is not None else random.choice(SERVICES)
    resolved_level = level if level is not None else random.choice(WEIGHTED_LEVELS)
    message = _create_message(resolved_service, resolved_level)
    return SystemLog(
        event_id=uuid4(),
        service_name=resolved_service,
        log_level=resolved_level,
        message=message,
    )


def send_log(
    log_entry: SystemLog,
    producer: Producer | None = None,
    topic: str = KAFKA_TOPIC_SYSTEM_LOGS,
) -> SystemLog:
    """Publish a single SystemLog event to Kafka keyed by service_name."""
    owned_producer = producer is None
    active_producer = (
        producer if producer is not None
        else Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    )
    active_producer.produce(
        topic,
        key=log_entry.service_name.encode("utf-8"),
        value=log_entry.model_dump_json().encode("utf-8"),
        callback=delivery_report,
    )
    active_producer.poll(0)
    if owned_producer:
        active_producer.flush()
    return log_entry


def generate_and_send(
    producer: Producer | None = None,
    topic: str = KAFKA_TOPIC_SYSTEM_LOGS,
) -> SystemLog:
    """Generate a synthetic log and publish it to Kafka."""
    log_entry = generate_mock_log()
    return send_log(log_entry, producer=producer, topic=topic)


def generate_and_send_burst(
    count: int,
    producer: Producer | None = None,
    topic: str = KAFKA_TOPIC_SYSTEM_LOGS,
) -> list[SystemLog]:
    """Generate and publish a batch burst of synthetic logs."""
    if count <= 0:
        return []
    owned_producer = producer is None
    active_producer = (
        producer if producer is not None
        else Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    )
    logs: list[SystemLog] = []
    for _ in range(count):
        log_entry = generate_mock_log()
        send_log(log_entry, producer=active_producer, topic=topic)
        logs.append(log_entry)
    if owned_producer:
        active_producer.flush()
    return logs


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Kafka synthetic log producer.")
    parser.add_argument("--burst", type=int, default=None,
                        help="Send a fixed burst of N logs and exit.")
    args = parser.parse_args(argv if argv is not None else [])
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    kafka_producer = Producer(conf)

    if args.burst is not None:
        print(f"Sending burst of {args.burst} logs...")
        generate_and_send_burst(args.burst, producer=kafka_producer)
        kafka_producer.flush()
        return

    print("Starting log stream... (Press Ctrl+C to stop)")
    try:
        while True:
            generate_and_send(producer=kafka_producer)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    kafka_producer.flush()


if __name__ == "__main__":
    main(sys.argv[1:])