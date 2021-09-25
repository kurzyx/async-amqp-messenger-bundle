<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use InvalidArgumentException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use React\EventLoop\LoopInterface as EventLoopInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class AsyncAmqpTransportFactory implements TransportFactoryInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private EventLoopInterface $eventLoop;

    public function __construct(EventLoopInterface $eventLoop)
    {
        $this->eventLoop = $eventLoop;
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (! str_starts_with($dsn, 'async-')) {
            throw new InvalidArgumentException('Expected DSN to start with "async-".');
        }

        $connection = Connection::fromDsn(
            substr($dsn, 6),
            $this->eventLoop
        );

        if ($this->logger !== null) {
            $connection->setLogger($this->logger);
        }

        $transporter = new AsyncAmqpTransport(
            $connection,
            new Config($options),
            $serializer,
        );

        if ($this->logger !== null) {
            $transporter->setLogger($this->logger);
        }

        return $transporter;
    }

    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'async-amqp://') || str_starts_with($dsn, 'async-amqps://');
    }
}
