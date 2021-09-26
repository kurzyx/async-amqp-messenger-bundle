<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Kurzyx\AsyncAmqpMessengerBundle\Connection\ConnectionInterface;
use Kurzyx\AsyncMessengerBundle\TerminableAsyncReceiverInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class AsyncAmqpTransport implements TerminableAsyncReceiverInterface, QueueReceiverInterface, TransportInterface, SetupableTransportInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private Config $config;
    private AsyncAmqpReceiver $receiver;
    private AsyncAmqpSender $sender;
    private SchemaHelper $schemaHelper;

    public function __construct(ConnectionInterface $connection, Config $config, SerializerInterface $serializer)
    {
        $this->config = $config;
        $this->receiver = new AsyncAmqpReceiver($connection, $config, $serializer);
        $this->sender = new AsyncAmqpSender($connection, $config, $serializer);
        $this->schemaHelper = new SchemaHelper($connection, $config);
    }

    /**
     * {@inheritDoc}
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->receiver->setLogger($logger);
    }

    /**
     * {@inheritDoc}
     */
    public function get(): iterable
    {
        if ($this->config->shouldAutoSetup()) {
            $this->schemaHelper->setup();
        }

        return $this->receiver->get();
    }

    /**
     * {@inheritDoc}
     */
    public function getFromQueues(array $queueNames): iterable
    {
        if ($this->config->shouldAutoSetup()) {
            $this->schemaHelper->setup();
        }

        return $this->receiver->getFromQueues($queueNames);
    }

    /**
     * {@inheritDoc}
     */
    public function ack(Envelope $envelope): void
    {
        $this->receiver->ack($envelope);
    }

    /**
     * {@inheritDoc}
     */
    public function reject(Envelope $envelope): void
    {
        $this->receiver->reject($envelope);
    }

    /**
     * {@inheritDoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        if ($this->config->shouldAutoSetup()) {
            $this->schemaHelper->setup();
        }

        return $this->sender->send($envelope);
    }

    /**
     * {@inheritDoc}
     */
    public function setup(): void
    {
        $this->schemaHelper->setup();
    }

    /**
     * {@inheritDoc}
     */
    public function terminateAsync(): void
    {
        $this->receiver->terminateAsync();
    }

    /**
     * {@inheritDoc}
     */
    public function setOnEnvelopePendingCallback(?callable $callback): void
    {
        $this->receiver->setOnEnvelopePendingCallback($callback);
    }
}
