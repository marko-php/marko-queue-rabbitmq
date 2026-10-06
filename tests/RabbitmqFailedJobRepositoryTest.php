<?php

declare(strict_types=1);

namespace Marko\Queue\Rabbitmq\Tests;

use DateTimeImmutable;
use JsonException;
use Marko\Queue\FailedJob;
use Marko\Queue\FailedJobRepositoryInterface;
use Marko\Queue\Rabbitmq\RabbitmqConnection;
use Marko\Queue\Rabbitmq\RabbitmqFailedJobRepository;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;

/** @noinspection PhpMissingParentConstructorInspection - Test stub intentionally skips parent */
class MockFailedJobChannel extends AMQPChannel
{
    /** @var list<array{body: string, properties: array<string, mixed>}> */
    public array $publishedMessages = [];

    /** @var list<int> */
    public array $ackedTags = [];

    /** @var list<array{tag: int, multiple: bool, requeue: bool}> */
    public array $nackedTags = [];

    public int $purgeCount = 0;

    public int $passiveDeclareCount = 0;

    /**
     * Ready messages, in queue order, like a broker queue.
     *
     * @var list<array{body: string, message_id: ?string, seq: int}>
     */
    public array $queuedMessages = [];

    /**
     * Messages handed out by basic_get() and not yet acked or nacked, keyed by delivery tag.
     *
     * @var array<int, array{body: string, message_id: ?string, seq: int}>
     */
    public array $unackedMessages = [];

    private int $nextDeliveryTag = 1;

    private int $nextSeq = 1;

    /** @noinspection PhpMissingParentConstructorInspection */
    public function __construct() {}

    /**
     * Put a message on the queue without recording it as published, e.g. a corrupt one.
     */
    public function enqueue(
        string $body,
        ?string $messageId,
    ): void {
        $this->queuedMessages[] = ['body' => $body, 'message_id' => $messageId, 'seq' => $this->nextSeq++];
    }

    /**
     * @return list<?string>
     */
    public function queuedMessageIds(): array
    {
        return array_column($this->queuedMessages, 'message_id');
    }

    public function queue_declare(
        $queue = '',
        $passive = false,
        $durable = false,
        $exclusive = false,
        $auto_delete = true,
        $nowait = false,
        $arguments = [],
        $ticket = null,
    ): ?array {
        if ($passive) {
            return [$queue, $this->passiveDeclareCount, 0];
        }

        return [$queue, 0, 0];
    }

    public function basic_publish(
        $msg,
        $exchange = '',
        $routing_key = '',
        $mandatory = false,
        $immediate = false,
        $ticket = null,
    ): void {
        $this->publishedMessages[] = [
            'body' => $msg->getBody(),
            'properties' => $msg->get_properties(),
        ];

        $this->enqueue($msg->getBody(), $msg->get('message_id'));
    }

    public function basic_get(
        $queue = '',
        $no_ack = false,
        $ticket = null,
    ): ?AMQPMessage {
        $messageData = array_shift($this->queuedMessages);

        if ($messageData === null) {
            return null;
        }

        $deliveryTag = $this->nextDeliveryTag++;
        $this->unackedMessages[$deliveryTag] = $messageData;

        $msg = new AMQPMessage(
            $messageData['body'],
            $messageData['message_id'] === null ? [] : ['message_id' => $messageData['message_id']],
        );
        $msg->setDeliveryTag($deliveryTag);

        return $msg;
    }

    public function basic_ack(
        $delivery_tag,
        $multiple = false,
    ): void {
        $this->ackedTags[] = $delivery_tag;
        $this->settle($delivery_tag);
    }

    public function basic_nack(
        $delivery_tag,
        $multiple = false,
        $requeue = false,
    ): void {
        $this->nackedTags[] = [
            'tag' => $delivery_tag,
            'multiple' => $multiple,
            'requeue' => $requeue,
        ];
        $message = $this->settle($delivery_tag);

        if ($requeue) {
            $this->queuedMessages[] = $message;
            usort($this->queuedMessages, fn (array $a, array $b): int => $a['seq'] <=> $b['seq']);
        }
    }

    /**
     * @return array{body: string, message_id: ?string, seq: int}
     */
    private function settle(
        int $deliveryTag,
    ): array {
        if (!isset($this->unackedMessages[$deliveryTag])) {
            throw new RuntimeException("PRECONDITION_FAILED - unknown delivery tag $deliveryTag");
        }

        $message = $this->unackedMessages[$deliveryTag];
        unset($this->unackedMessages[$deliveryTag]);

        return $message;
    }

    public function queue_purge(
        $queue = '',
        $nowait = false,
        $ticket = null,
    ): ?int {
        $count = count($this->queuedMessages);
        $this->queuedMessages = [];
        $this->purgeCount++;

        return $count;
    }
}

function createFailedJobTestConnection(
    MockFailedJobChannel $mockChannel,
): RabbitmqConnection {
    return new class ($mockChannel) extends RabbitmqConnection
    {
        public function __construct(
            private readonly MockFailedJobChannel $mockChannel,
        ) {
            parent::__construct();
        }

        protected function createConnection(): AbstractConnection
        {
            /** @noinspection PhpMissingParentConstructorInspection - Test stub intentionally skips parent */
            return new class ($this->mockChannel) extends AbstractConnection
            {
                /** @noinspection PhpMissingParentConstructorInspection */
                public function __construct(
                    private readonly AMQPChannel $mockChannel,
                ) {}

                public function channel(
                    $channel_id = null,
                ): AMQPChannel {
                    return $this->mockChannel;
                }

                public function isConnected(): bool
                {
                    return true;
                }
            };
        }
    };
}

test('it implements FailedJobRepositoryInterface', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);

    $repository = new RabbitmqFailedJobRepository($connection);

    expect($repository)->toBeInstanceOf(FailedJobRepositoryInterface::class);
});

test('it stores failed job as message in failed jobs queue', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $failedJob = new FailedJob(
        id: 'failed-123',
        queue: 'default',
        payload: '{"class":"TestJob","data":{}}',
        exception: 'RuntimeException: Test error',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );

    $repository->store($failedJob);

    expect($channel->publishedMessages)->toHaveCount(1);

    $body = json_decode($channel->publishedMessages[0]['body'], true);

    expect($body)->toHaveKey('id', 'failed-123')
        ->and($body)->toHaveKey('queue', 'default')
        ->and($body)->toHaveKey('payload', '{"class":"TestJob","data":{}}')
        ->and($body)->toHaveKey('exception', 'RuntimeException: Test error')
        ->and($body)->toHaveKey('failedAt', '2024-01-15T10:30:00+00:00');
});

test('it stores job ID as message ID property', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $failedJob = new FailedJob(
        id: 'failed-456',
        queue: 'emails',
        payload: '{"class":"SendEmail"}',
        exception: 'RuntimeException: SMTP error',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($failedJob);

    $properties = $channel->publishedMessages[0]['properties'];

    expect($properties)->toHaveKey('message_id', 'failed-456')
        ->and($properties)->toHaveKey('delivery_mode', AMQPMessage::DELIVERY_MODE_PERSISTENT);
});

test('it retrieves all failed jobs from queue', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'failed-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $failedJobs = $repository->all();

    expect($failedJobs)->toHaveCount(2)
        ->and($failedJobs[0])->toBeInstanceOf(FailedJob::class)
        ->and($failedJobs[0]->id)->toBe('failed-1')
        ->and($failedJobs[0]->queue)->toBe('default')
        ->and($failedJobs[1]->id)->toBe('failed-2')
        ->and($failedJobs[1]->queue)->toBe('emails');
});

test('it returns empty array when no failed jobs exist', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $failedJobs = $repository->all();

    expect($failedJobs)->toBeEmpty();
});

test('it finds failed job by ID', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'failed-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $found = $repository->find('failed-2');

    expect($found)->toBeInstanceOf(FailedJob::class)
        ->and($found->id)->toBe('failed-2')
        ->and($found->queue)->toBe('emails')
        ->and($found->payload)->toBe('{"class":"Job2"}')
        ->and($found->exception)->toBe('Error 2');
});

test('it returns null when failed job not found', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );

    $repository->store($job);

    $found = $repository->find('non-existent');

    expect($found)->toBeNull();
});

test('it deletes failed job by ID and returns true', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'failed-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $result = $repository->delete('failed-1');

    // Only the deleted message is acked; the other is requeued
    expect($result)->toBeTrue()
        ->and($channel->ackedTags)->toHaveCount(1)
        ->and($channel->queuedMessageIds())->toBe(['failed-2']);
});

test('it returns false when deleting non-existent failed job', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );

    $repository->store($job);

    $result = $repository->delete('non-existent');

    // Non-matching target: nothing acked, every message requeued
    expect($result)->toBeFalse()
        ->and($channel->ackedTags)->toBeEmpty()
        ->and($channel->queuedMessageIds())->toBe(['failed-1']);
});

test('it clears all failed jobs via queue purge', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'failed-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'failed-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $count = $repository->clear();

    expect($count)->toBe(2)
        ->and($channel->purgeCount)->toBe(1);
});

test('it counts failed jobs via passive queue declare', function (): void {
    $channel = new MockFailedJobChannel();
    $channel->passiveDeclareCount = 5;
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $count = $repository->count();

    expect($count)->toBe(5);
});

it('sets the AMQP message_id to the failed job id when storing a failed job', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $failedJob = new FailedJob(
        id: 'job-uuid-001',
        queue: 'default',
        payload: '{"class":"TestJob"}',
        exception: 'RuntimeException: fail',
        failedAt: new DateTimeImmutable('2024-03-01T12:00:00+00:00'),
    );

    $repository->store($failedJob);

    $properties = $channel->publishedMessages[0]['properties'];

    expect($properties['message_id'])->toBe('job-uuid-001');
});

it('deletes only the matching failed job and returns true, leaving the others intact', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'del-keep-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'del-target-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $result = $repository->delete('del-target-2');

    // Returns true for found target
    expect($result)->toBeTrue()
        // Only the target is acked; the other is requeued, never republished
        ->and($channel->ackedTags)->toHaveCount(1)
        ->and($channel->nackedTags)->toHaveCount(1)
        ->and($channel->nackedTags[0]['requeue'])->toBeTrue()
        ->and($channel->publishedMessages)->toHaveCount(2)
        ->and($channel->queuedMessageIds())->toBe(['del-keep-1']);
});

it('returns false from delete() when no failed job matches the id', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job = new FailedJob(
        id: 'del-false-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );

    $repository->store($job);

    $result = $repository->delete('no-such-id');

    expect($result)->toBeFalse();
});

it('returns null from find() when no failed job matches the id', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job = new FailedJob(
        id: 'find-null-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );

    $repository->store($job);

    $found = $repository->find('does-not-exist');

    expect($found)->toBeNull();
});

it('returns the matching failed job from find() by id and terminates', function (): void {
    $channel = new MockFailedJobChannel();
    $connection = createFailedJobTestConnection($channel);
    $repository = new RabbitmqFailedJobRepository($connection);

    $job1 = new FailedJob(
        id: 'find-term-1',
        queue: 'default',
        payload: '{"class":"Job1"}',
        exception: 'Error 1',
        failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
    );
    $job2 = new FailedJob(
        id: 'find-term-2',
        queue: 'emails',
        payload: '{"class":"Job2"}',
        exception: 'Error 2',
        failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
    );

    $repository->store($job1);
    $repository->store($job2);

    $found = $repository->find('find-term-2');

    // Returns correct job
    expect($found)->toBeInstanceOf(FailedJob::class)
        ->and($found->id)->toBe('find-term-2')
        // find is read-only: nothing acked, both messages requeued in their original order
        ->and($channel->ackedTags)->toBeEmpty()
        ->and($channel->nackedTags)->toHaveCount(2)
        ->and($channel->publishedMessages)->toHaveCount(2)
        ->and($channel->queuedMessageIds())->toBe(['find-term-1', 'find-term-2']);
});

it(
    'returns all stored failed jobs from all() and the call terminates (requeues only after the drain, so no basic_get/basic_nack loop)',
    function (): void {
        $channel = new MockFailedJobChannel();
        $connection = createFailedJobTestConnection($channel);
        $repository = new RabbitmqFailedJobRepository($connection);

        $job1 = new FailedJob(
            id: 'term-1',
            queue: 'default',
            payload: '{"class":"Job1"}',
            exception: 'Error 1',
            failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
        );
        $job2 = new FailedJob(
            id: 'term-2',
            queue: 'emails',
            payload: '{"class":"Job2"}',
            exception: 'Error 2',
            failedAt: new DateTimeImmutable('2024-01-15T11:00:00+00:00'),
        );

        $repository->store($job1);
        $repository->store($job2);

        $failedJobs = $repository->all();

        // Returns correct jobs
        expect($failedJobs)->toHaveCount(2)
            ->and($failedJobs[0]->id)->toBe('term-1')
            ->and($failedJobs[1]->id)->toBe('term-2')
            // Read-only: nothing acked, every message requeued, nothing republished
            ->and($channel->ackedTags)->toBeEmpty()
            ->and($channel->nackedTags)->toHaveCount(2)
            ->and($channel->publishedMessages)->toHaveCount(2)
            ->and($channel->queuedMessageIds())->toBe(['term-1', 'term-2']);
    },
);

function storeFailedJobs(
    RabbitmqFailedJobRepository $repository,
    string ...$ids,
): void {
    foreach ($ids as $id) {
        $repository->store(new FailedJob(
            id: $id,
            queue: 'default',
            payload: '{"class":"Job"}',
            exception: "Error $id",
            failedAt: new DateTimeImmutable('2024-01-15T10:30:00+00:00'),
        ));
    }
}

describe('message safety', function (): void {
    it('keeps every failed job on the queue when a message fails to deserialize mid-drain', function (): void {
        $channel = new MockFailedJobChannel();
        $repository = new RabbitmqFailedJobRepository(createFailedJobTestConnection($channel));
        storeFailedJobs($repository, 'before');
        $channel->enqueue('{not json', 'corrupt');
        storeFailedJobs($repository, 'after-1', 'after-2');

        expect(fn () => $repository->all())->toThrow(JsonException::class)
            ->and($channel->ackedTags)->toBeEmpty()
            ->and($channel->unackedMessages)->toBeEmpty()
            ->and($channel->queuedMessageIds())->toBe(['before', 'corrupt', 'after-1', 'after-2']);
    });

    it('keeps every failed job when find() hits a corrupt matching message', function (): void {
        $channel = new MockFailedJobChannel();
        $repository = new RabbitmqFailedJobRepository(createFailedJobTestConnection($channel));
        $channel->enqueue('{not json', 'corrupt');
        storeFailedJobs($repository, 'other');

        expect(fn () => $repository->find('corrupt'))->toThrow(JsonException::class)
            ->and($channel->queuedMessageIds())->toBe(['corrupt', 'other']);
    });

    it('returns the same failed jobs on repeated reads', function (): void {
        $channel = new MockFailedJobChannel();
        $repository = new RabbitmqFailedJobRepository(createFailedJobTestConnection($channel));
        storeFailedJobs($repository, 'a', 'b');

        $first = array_map(fn (FailedJob $job): string => $job->id, $repository->all());
        $repository->find('a');
        $second = array_map(fn (FailedJob $job): string => $job->id, $repository->all());

        expect($first)->toBe(['a', 'b'])
            ->and($second)->toBe(['a', 'b']);
    });

    it('acks only the deleted message and leaves the rest in order', function (): void {
        $channel = new MockFailedJobChannel();
        $repository = new RabbitmqFailedJobRepository(createFailedJobTestConnection($channel));
        storeFailedJobs($repository, 'a', 'b', 'c');

        expect($repository->delete('b'))->toBeTrue()
            ->and($channel->queuedMessageIds())->toBe(['a', 'c'])
            ->and($channel->unackedMessages)->toBeEmpty()
            ->and($channel->ackedTags)->toHaveCount(1);
    });
});
