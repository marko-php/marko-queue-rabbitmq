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

    public static function defaultCredentialsOnRemoteHost(
        string $host,
    ): self {
        return new self(
            message: "Refusing to connect to RabbitMQ at '$host' with the default guest/guest credentials.",
            context: 'Creating the AMQP connection for marko/queue-rabbitmq. guest/guest is only accepted for a loopback host (localhost, 127.0.0.1, ::1).',
            suggestion: 'Create a dedicated RabbitMQ user for this application and set RABBITMQ_USER and RABBITMQ_PASSWORD (or user and password in config/queue-rabbitmq.php). Enable TLS with the tls option when the broker is reached over a network.',
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

    public static function malformedMessage(
        string $queue,
        string $reason,
    ): self {
        return new self(
            message: "Malformed message on RabbitMQ queue '$queue': $reason.",
            context: 'Reading a message popped from RabbitMQ before handing its job to the worker.',
            suggestion: 'Publish jobs only through QueueInterface::push() or later(), which set the job_id header and sign the payload.',
        );
    }

    public static function republishNotConfirmed(
        string $jobId,
        string $queue,
    ): self {
        return new self(
            message: "RabbitMQ did not confirm the retry of job '$jobId' on queue '$queue'.",
            context: 'Releasing a failed job: the broker nacked the republished copy, so the original message was requeued instead of acknowledged.',
            suggestion: 'Check the RabbitMQ server logs and resource alarms (disk or memory); the job will be delivered again from its original message.',
        );
    }
}
