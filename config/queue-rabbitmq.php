<?php

declare(strict_types=1);

use Marko\Config\Env;

return [
    'host' => Env::string('RABBITMQ_HOST', 'localhost'),
    'port' => Env::int('RABBITMQ_PORT', 5672, min: 1, max: 65535),
    'user' => Env::string('RABBITMQ_USER', 'guest'),
    'password' => Env::string('RABBITMQ_PASSWORD', 'guest'),
    'vhost' => Env::string('RABBITMQ_VHOST', '/'),
    // SSL stream context options (e.g. ['cafile' => '/path/ca.pem', 'verify_peer' => true]), or null for plain TCP
    'tls' => null,
    'exchange' => [
        'name' => Env::string('RABBITMQ_EXCHANGE', 'marko'),
        'type' => Env::string('RABBITMQ_EXCHANGE_TYPE', 'direct'),
        'durable' => true,
        'auto_delete' => false,
    ],
];
