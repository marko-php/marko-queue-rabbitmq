<?php

declare(strict_types=1);

namespace Marko\Queue\Rabbitmq\Exceptions;

use Marko\Queue\Exceptions\QueueException;
use Throwable;

class RabbitmqException extends QueueException
{
    public static function connectionFailed(
        string $host,
        int $port,
        Throwable $previous,
    ): self {
        return new self(
            message: "Could not connect to RabbitMQ at $host:$port: {$previous->getMessage()}",
            context: 'Opening the AMQP connection for marko/queue-rabbitmq.',
            suggestion: 'Check the connection settings in config/queue-rabbitmq.php, or set the RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD and RABBITMQ_VHOST environment variables, and make sure the RabbitMQ server is reachable.',
            previous: $previous,
        );
    }

    /**
     * @param list<string> $validTypes
     */
    public static function invalidExchangeType(
        string $type,
        array $validTypes,
    ): self {
        $valid = implode(', ', $validTypes);

        return new self(
            message: "Invalid RabbitMQ exchange type '$type'.",
            context: 'Reading queue-rabbitmq.exchange.type from config.',
            suggestion: "Set queue-rabbitmq.exchange.type (or RABBITMQ_EXCHANGE_TYPE) to one of: $valid.",
        );
    }
}
