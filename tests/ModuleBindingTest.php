<?php

declare(strict_types=1);

namespace Marko\Queue\Rabbitmq\Tests;

use Marko\Config\ConfigRepositoryInterface;
use Marko\Core\Container\Container;
use Marko\Queue\QueueInterface;
use Marko\Queue\Rabbitmq\Exceptions\RabbitmqException;
use Marko\Queue\Rabbitmq\Exchange\ExchangeConfig;
use Marko\Queue\Rabbitmq\Exchange\ExchangeType;
use Marko\Queue\Rabbitmq\RabbitmqConnection;
use Marko\Queue\Rabbitmq\RabbitmqQueue;
use Marko\Testing\Fake\FakeConfigRepository;
use ReflectionProperty;

/**
 * @param array<string, mixed> $overrides
 * @param list<string> $without Config keys to leave out
 */
function createRabbitmqContainer(
    array $overrides = [],
    array $without = [],
): Container {
    $config = [
        'queue.queue' => 'emails',
        'encryption.key' => 'test-key',
        'queue-rabbitmq.host' => 'rabbit.internal',
        'queue-rabbitmq.port' => 5671,
        'queue-rabbitmq.user' => 'app',
        'queue-rabbitmq.password' => 'secret',
        'queue-rabbitmq.vhost' => '/app',
        'queue-rabbitmq.tls' => ['verify_peer' => true],
        'queue-rabbitmq.exchange.name' => 'app-exchange',
        'queue-rabbitmq.exchange.type' => 'topic',
        'queue-rabbitmq.exchange.durable' => false,
        'queue-rabbitmq.exchange.auto_delete' => true,
        ...$overrides,
    ];

    foreach ($without as $key) {
        unset($config[$key]);
    }

    $container = new Container();
    $container->instance(ConfigRepositoryInterface::class, new FakeConfigRepository($config));

    $modules = [
        require dirname(__DIR__, 2) . '/queue/module.php',
        require dirname(__DIR__) . '/module.php',
    ];

    foreach ($modules as $module) {
        foreach ($module['bindings'] as $id => $implementation) {
            $container->bind($id, $implementation);
        }

        foreach ($module['singletons'] ?? [] as $id) {
            $container->singleton($id);
        }
    }

    return $container;
}

describe('queue-rabbitmq module bindings', function (): void {
    it('ships a queue-rabbitmq config file with connection and exchange defaults', function (): void {
        $config = require dirname(__DIR__) . '/config/queue-rabbitmq.php';

        expect($config)->toHaveKeys(['host', 'port', 'user', 'password', 'vhost', 'tls', 'exchange'])
            ->and($config['port'])->toBeInt()
            ->and($config['tls'])->toBeNull()
            ->and($config['exchange'])->toBe([
                'name' => 'marko',
                'type' => 'direct',
                'durable' => true,
                'auto_delete' => false,
            ]);
    });

    it('resolves RabbitmqConnection with values from queue-rabbitmq config', function (): void {
        $connection = createRabbitmqContainer()->get(RabbitmqConnection::class);

        expect($connection->host)->toBe('rabbit.internal')
            ->and($connection->port)->toBe(5671)
            ->and($connection->user)->toBe('app')
            ->and($connection->password)->toBe('secret')
            ->and($connection->vhost)->toBe('/app')
            ->and($connection->tlsOptions)->toBe(['verify_peer' => true]);
    });

    it('passes a null tls config through as no TLS options', function (): void {
        $connection = createRabbitmqContainer(['queue-rabbitmq.tls' => null])->get(RabbitmqConnection::class);

        expect($connection->tlsOptions)->toBeNull();
    });

    it('treats a tls key removed by a null app override as no TLS options', function (): void {
        $connection = createRabbitmqContainer(without: ['queue-rabbitmq.tls'])->get(RabbitmqConnection::class);

        expect($connection->tlsOptions)->toBeNull();
    });

    it('resolves the same RabbitmqConnection instance twice', function (): void {
        $container = createRabbitmqContainer();

        expect($container->get(RabbitmqConnection::class))->toBe($container->get(RabbitmqConnection::class));
    });

    it('resolves ExchangeConfig from queue-rabbitmq exchange config', function (): void {
        $exchange = createRabbitmqContainer()->get(ExchangeConfig::class);

        expect($exchange->name)->toBe('app-exchange')
            ->and($exchange->type)->toBe(ExchangeType::Topic)
            ->and($exchange->durable)->toBeFalse()
            ->and($exchange->autoDelete)->toBeTrue();
    });

    it('throws RabbitmqException for an unknown exchange type', function (): void {
        $container = createRabbitmqContainer(['queue-rabbitmq.exchange.type' => 'bogus']);

        expect(fn () => $container->get(ExchangeConfig::class))
            ->toThrow(RabbitmqException::class, "Invalid RabbitMQ exchange type 'bogus'");
    });

    it('resolves QueueInterface with no hand-written bindings', function (): void {
        $container = createRabbitmqContainer();

        $queue = $container->get(QueueInterface::class);

        expect($queue)->toBeInstanceOf(RabbitmqQueue::class)
            ->and((new ReflectionProperty(RabbitmqQueue::class, 'connection'))->getValue($queue))
            ->toBe($container->get(RabbitmqConnection::class));
    });

    it('uses queue.queue as the default queue', function (): void {
        $queue = createRabbitmqContainer()->get(QueueInterface::class);

        expect((new ReflectionProperty(RabbitmqQueue::class, 'defaultQueue'))->getValue($queue))->toBe('emails');
    });
});
