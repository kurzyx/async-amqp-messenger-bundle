<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Symfony\Component\Messenger\Exception\InvalidArgumentException;

/**
 * @internal
 */
final class DsnParser
{
    public function parse(string $dsn): array
    {
        if (false === $parsedUrl = parse_url($dsn)) {
            // this is a valid URI that parse_url cannot handle when you want to pass all parameters as options
            if (! in_array($dsn, ['amqp://', 'amqps://'])) {
                throw new InvalidArgumentException(sprintf('The given AMQP DSN "%s" is invalid.', $dsn));
            }

            $parsedUrl = [];
        }

        $defaultPort = str_starts_with($dsn, 'amqps://') ? 5671 : 5672;
        $pathParts = isset($parsedUrl['path'])
            ? explode('/', trim($parsedUrl['path'], '/'))
            : [];

        $queryOptions = [];
        parse_str($parsedUrl['query'] ?? '', $queryOptions);

        return [
            'host'     => $parsedUrl['host'] ?? 'localhost',
            'port'     => $parsedUrl['port'] ?? $defaultPort,
            'vhost'    => isset($pathParts[0]) ? urldecode($pathParts[0]) : '/',
            'user'     => $parsedUrl['user'] ?? null,
            'password' => $parsedUrl['pass'] ?? null,
            'options'  => $queryOptions,
        ];
    }
}
