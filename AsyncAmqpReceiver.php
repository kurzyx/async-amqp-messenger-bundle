<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Closure;
use Kurzyx\AsyncAmqpMessengerBundle\Connection\ConnectionInterface;
use Kurzyx\AsyncAmqpMessengerBundle\Message\MessageInterface;
use Kurzyx\AsyncMessengerBundle\TerminableAsyncReceiverInterface;
use LogicException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Receiver\QueueReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class AsyncAmqpReceiver implements TerminableAsyncReceiverInterface, QueueReceiverInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private ConnectionInterface $connection;
    private Config $config;
    private SerializerInterface $serializer;
    private ?Closure $onEnvelopePendingCallback = null;

    /**
     * @var Envelope[][]
     */
    private array $pendingEnvelopesByQueue = [];

    public function __construct(ConnectionInterface $connection, Config $config, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->config = $config;
        $this->serializer = $serializer;
    }

    public function setOnEnvelopePendingCallback(?callable $callback): void
    {
        $this->onEnvelopePendingCallback = $callback !== null
            ? Closure::fromCallable($callback)
            : null;
    }

    public function get(): iterable
    {
        return $this->getFromQueues(array_keys($this->config->getQueues()));
    }

    public function getFromQueues(array $queueNames): iterable
    {
        foreach ($queueNames as $queueName) {
            $this->startConsumerIfNecessary($queueName);

            while (null !== ($envelope = $this->takeNextEnvelopeFromQueue($queueName))) {
                yield $envelope;
            }
        }
    }

    public function ack(Envelope $envelope): void
    {
        $receivedStamp = $this->findReceivedStamp($envelope);

        $this->connection->ack($receivedStamp->getMessage());
    }

    public function reject(Envelope $envelope): void
    {
        $receivedStamp = $this->findReceivedStamp($envelope);

        // Nack and don't requeue.
        $this->connection->nack($receivedStamp->getMessage(), false);
    }

    public function terminateAsync(): void
    {
        $this->connection->cancelConsumers();

        if ($this->logger !== null) {
            $pendingEnvelopesCount = array_reduce(
                $this->pendingEnvelopesByQueue,
                static fn(int $current, array $pendingEnvelopes) => $current + count($pendingEnvelopes),
                0
            );

            if ($pendingEnvelopesCount > 0) {
                $this->logger->warning(sprintf(
                    '%s pending envelopes are discarded. They will be automatically re-queued by AMQP.',
                    $pendingEnvelopesCount
                ));
            }
        }

        // We can simply discard all pending envelopes. Because they will never be acknowledged/rejected, they are
        // automatically re-queued by AMQP when the channel is closed.
        $this->pendingEnvelopesByQueue = [];
    }

    private function startConsumerIfNecessary(string $queueName): void
    {
        if ($this->connection->isConsuming($queueName)) {
            return;
        }

        $this->connection->consume(
            $queueName,
            fn(MessageInterface $message) => $this->onMessage($message, $queueName)
        );
    }

    private function onMessage(MessageInterface $message, string $queueName): void
    {
        $this->pendingEnvelopesByQueue[$queueName][] = $this->createEnvelope($message, $queueName);

        if ($this->onEnvelopePendingCallback !== null) {
            ($this->onEnvelopePendingCallback)();
        }
    }

    private function takeNextEnvelopeFromQueue(string $queueName): ?Envelope
    {
        if (empty($this->pendingEnvelopesByQueue[$queueName])) {
            return null;
        }

        $envelopes = array_splice($this->pendingEnvelopesByQueue[$queueName], 0, 1);

        return $envelopes[0] ?? null;
    }

    private function createEnvelope(MessageInterface $message, string $queueName): Envelope
    {
        try {
            $envelope = $this->serializer->decode([
                'body'    => $message->getContent(),
                'headers' => $message->getHeaders(),
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $this->connection->nack($message);

            throw $exception;
        }

        return $envelope->with(new AmqpReceivedStamp($message, $queueName));
    }

    private function findReceivedStamp(Envelope $envelope): AmqpReceivedStamp
    {
        /** @var AmqpReceivedStamp|null $stamp */
        $stamp = $envelope->last(AmqpReceivedStamp::class);
        if ($stamp === null) {
            throw new LogicException('No "AsyncAmqpReceivedStamp" stamp found on the envelope.');
        }

        return $stamp;
    }
}
