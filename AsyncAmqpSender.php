<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class AsyncAmqpSender implements SenderInterface
{
    private Connection $connection;
    private Config $config;
    private SerializerInterface $serializer;

    public function __construct(Connection $connection, Config $config, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->config = $config;
        $this->serializer = $serializer;
    }

    /**
     * {@inheritDoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $exchangeName = $this->config->getDefaultExchangeName();
        $routingKey = $this->config->getDefaultRoutingKey();

        /** @var AmqpRoutingStamp|null $amqpRoutingStamp */
        $amqpRoutingStamp = $envelope->last(AmqpRoutingStamp::class);
        if ($amqpRoutingStamp !== null) {
            $exchangeName = $amqpRoutingStamp->getExchangeName() ?? $exchangeName;
            $routingKey = $amqpRoutingStamp->getRoutingKey() ?? $routingKey;
        }

        if ($this->isRedelivery($envelope)) {
            return $this->publishRedelivery($envelope);
        }

        if (($delay = $this->getDelay($envelope)) > 0) {
            return $this->publishDelayed($delay, $envelope, $exchangeName, $routingKey);
        }

        return $this->publish($envelope, $exchangeName, $routingKey);
    }

    private function publish(Envelope $envelope, string $exchangeName, string $routingKey): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $this->connection->publish(
            $encodedMessage['body'],
            $encodedMessage['headers'] ?? [],
            $exchangeName,
            $routingKey
        );

        return $envelope;
    }

    private function publishRedelivery(Envelope $envelope): Envelope
    {
        /** @var AmqpReceivedStamp|null $amqpReceivedStamp */
        $amqpReceivedStamp = $envelope->last(AmqpReceivedStamp::class);
        if ($amqpReceivedStamp === null) {
            throw new RuntimeException('Unable to re-deliver envelope. AsyncAmqpReceivedStamp is missing.');
        }

        return $this->publishDelayed(
            $this->getDelay($envelope),
            $envelope,
            // Route any re-deliveries through the default exchange. This is a special exchange that routes the message
            // directly to a queue. The routing-key is used for the queue-name.
            '',
            $amqpReceivedStamp->getQueueName()
        );
    }

    private function publishDelayed(
        int $delay,
        Envelope $envelope,
        string $targetExchangeName,
        string $targetRoutingKey
    ): Envelope {
        $delayExchangeName = $this->config->getDelayExchangeName();
        $delayRoutingKey = $this->getRoutingKeyForDelay($targetExchangeName, $targetRoutingKey, $delay);

        $this->declareDelayQueue(
            $delay,
            $targetExchangeName,
            $targetRoutingKey,
            $delayExchangeName,
            $delayRoutingKey,
        );

        return $this->publish($envelope, $delayExchangeName, $delayRoutingKey);
    }

    private function getRoutingKeyForDelay(string $exchangeName, string $routingKey, int $delay): string
    {
        return str_replace(
            ['%delay%', '%exchange_name%', '%routing_key%'],
            [$delay, $exchangeName, $routingKey],
            $this->config->getDelayQueueNamePattern()
        );
    }

    private function getDelay(Envelope $envelope): int
    {
        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);

        return $delayStamp ? $delayStamp->getDelay() : 0;
    }

    private function isRedelivery(Envelope $envelope): bool
    {
        /** @var RedeliveryStamp|null $redeliveryStamp */
        $redeliveryStamp = $envelope->last(RedeliveryStamp::class);

        return $redeliveryStamp !== null;
    }

    private function declareDelayQueue(
        int $delay,
        string $originalExchangeName,
        ?string $originalRoutingKey,
        string $delayExchangeName,
        string $delayRoutingKey
    ): void {
        $this->connection->queueDeclare(
            $delayRoutingKey,
            false,
            true,
            false,
            false,
            [
                'x-message-ttl'             => $delay,
                // Delete the delay queue 10 seconds after the message expires.
                // Publishing another message re-declares the queue which renews the lease.
                'x-expires'                 => $delay + 10000,
                'x-dead-letter-exchange'    => $originalExchangeName,
                'x-dead-letter-routing-key' => $originalRoutingKey ?? '',
            ]
        );
        $this->connection->queueBind($delayRoutingKey, $delayExchangeName, $delayRoutingKey);
    }
}
