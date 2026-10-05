<?php

declare(strict_types=1);

return [
    'host' => $_ENV['RABBITMQ_HOST'] ?? 'localhost',
    'port' => (int) ($_ENV['RABBITMQ_PORT'] ?? 5672),
    'user' => $_ENV['RABBITMQ_USER'] ?? 'guest',
    'password' => $_ENV['RABBITMQ_PASSWORD'] ?? 'guest',
    'vhost' => $_ENV['RABBITMQ_VHOST'] ?? '/',
    // SSL stream context options (e.g. ['cafile' => '/path/ca.pem', 'verify_peer' => true]), or null for plain TCP
    'tls' => null,
    'exchange' => [
        'name' => $_ENV['RABBITMQ_EXCHANGE'] ?? 'marko',
        'type' => $_ENV['RABBITMQ_EXCHANGE_TYPE'] ?? 'direct',
        'durable' => true,
        'auto_delete' => false,
    ],
];
