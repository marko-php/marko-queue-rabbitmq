<?php

declare(strict_types=1);

namespace Marko\Queue\Rabbitmq;

use DateMalformedStringException;
use DateTimeImmutable;
use Exception;
use JsonException;
use Marko\Queue\FailedJob;
use Marko\Queue\FailedJobRepositoryInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Stores failed jobs as messages on the failed_jobs queue.
 *
 * Reads drain the queue without acknowledging anything, then hand every message back with
 * basic_nack(requeue: true), and delete() acks only the matching message. A message is never
 * acked before the read is done with it, so an error partway through, or a crash, leaves every
 * failed job on the queue: the broker requeues unacked messages when the channel closes.
 */
readonly class RabbitmqFailedJobRepository implements FailedJobRepositoryInterface
{
    private const string QUEUE_NAME = 'failed_jobs';

    public function __construct(
        private RabbitmqConnection $connection,
    ) {}

    /**
     * @throws JsonException|Exception
     */
    public function store(
        FailedJob $failedJob,
    ): void {
        $channel = $this->connection->channel();

        $channel->queue_declare(
            self::QUEUE_NAME,
            durable: true,
            auto_delete: false,
        );

        $body = json_encode([
            'id' => $failedJob->id,
            'queue' => $failedJob->queue,
            'payload' => $failedJob->payload,
            'exception' => $failedJob->exception,
            'failedAt' => $failedJob->failedAt->format('c'),
        ], JSON_THROW_ON_ERROR);

        $channel->basic_publish(
            new AMQPMessage($body, [
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'message_id' => $failedJob->id,
            ]),
            '',
            self::QUEUE_NAME,
        );
    }

    /**
     * @return list<FailedJob>
     *
     * @throws DateMalformedStringException|JsonException|Exception
     */
    public function all(): array
    {
        $channel = $this->connection->channel();
        $drained = $this->drain($channel);

        try {
            return array_map($this->deserializeMessage(...), $drained);
        } finally {
            $this->requeue($channel, $drained);
        }
    }

    /**
     * @throws DateMalformedStringException|JsonException|Exception
     */
    public function find(
        string $id,
    ): ?FailedJob {
        $channel = $this->connection->channel();
        $drained = $this->drain($channel);

        try {
            foreach ($drained as $message) {
                if ($message->get('message_id') === $id) {
                    return $this->deserializeMessage($message);
                }
            }

            return null;
        } finally {
            $this->requeue($channel, $drained);
        }
    }

    /**
     * @throws Exception
     */
    public function delete(
        string $id,
    ): bool {
        $channel = $this->connection->channel();
        $matched = [];
        $kept = [];

        foreach ($this->drain($channel) as $message) {
            if ($message->get('message_id') === $id) {
                $matched[] = $message;
            } else {
                $kept[] = $message;
            }
        }

        try {
            foreach ($matched as $message) {
                $channel->basic_ack($message->getDeliveryTag());
            }
        } finally {
            $this->requeue($channel, $kept);
        }

        return $matched !== [];
    }

    /**
     * @throws Exception
     */
    public function clear(): int
    {
        $channel = $this->connection->channel();

        return (int) $channel->queue_purge(self::QUEUE_NAME);
    }

    /**
     * @throws Exception
     */
    public function count(): int
    {
        $channel = $this->connection->channel();

        [, $messageCount] = $channel->queue_declare(self::QUEUE_NAME, passive: true);

        return (int) $messageCount;
    }

    /**
     * Fetch every message on the queue without acknowledging any of them.
     *
     * The broker does not hand an unacked message out again while this channel holds it, so the
     * loop ends once the queue is empty. Every drained message must be acked or requeued afterwards.
     *
     * @return list<AMQPMessage>
     *
     * @throws Exception
     */
    private function drain(
        AMQPChannel $channel,
    ): array {
        $messages = [];

        while (($message = $channel->basic_get(self::QUEUE_NAME)) !== null) {
            $messages[] = $message;
        }

        return $messages;
    }

    /**
     * Hand drained messages back to the queue unchanged.
     *
     * @param list<AMQPMessage> $messages
     *
     * @throws Exception
     */
    private function requeue(
        AMQPChannel $channel,
        array $messages,
    ): void {
        foreach ($messages as $message) {
            $channel->basic_nack($message->getDeliveryTag(), requeue: true);
        }
    }

    /**
     * @throws DateMalformedStringException|JsonException
     */
    private function deserializeMessage(
        AMQPMessage $message,
    ): FailedJob {
        $data = json_decode($message->getBody(), true, 512, JSON_THROW_ON_ERROR);

        return new FailedJob(
            id: $data['id'],
            queue: $data['queue'],
            payload: $data['payload'],
            exception: $data['exception'],
            failedAt: new DateTimeImmutable($data['failedAt']),
        );
    }
}
