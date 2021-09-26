<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Connection;

use Bunny\Async\Client as AsyncClient;
use Bunny\Channel;
use Bunny\Client as SyncClient;
use Bunny\ClientStateEnum;
use Bunny\Message;
use Bunny\Protocol\MethodBasicConsumeOkFrame;
use DateTime;
use Kurzyx\AsyncAmqpMessengerBundle\Message\BunnyMessage;
use Kurzyx\AsyncAmqpMessengerBundle\Message\MessageInterface;
use LogicException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use React\EventLoop\LoopInterface as EventLoopInterface;
use React\Promise\ExtendedPromiseInterface;
use React\Promise\PromiseInterface;
use RuntimeException;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use function React\Promise\resolve;

final class BunnyConnection implements ConnectionInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private const CONSUMER_STATUS_PENDING = 'pending';
    private const CONSUMER_STATUS_RUNNING = 'running';

    private static int $channelIdIncrement = 0;

    private SyncClient $syncClient;
    private AsyncClient $asyncClient;

    private ?PromiseInterface $asyncConnectionPromise = null;

    /**
     * @var Channel[]
     */
    private array $channelById = [];

    /**
     * @var int[]
     */
    private array $idByChannel = [];

    /**
     * @var int|null
     */
    private ?int $publishChannelId = null;

    /**
     * @var string[]
     */
    private array $consumerStatusByQueueName = [];

    /**
     * @var string[][]
     */
    private array $consumerTagsByChannelId = [];

    private function __construct(EventLoopInterface $eventLoop, array $clientOptions)
    {
        $this->syncClient = new SyncClient($clientOptions);
        $this->asyncClient = new AsyncClient($eventLoop, $clientOptions);
    }

    /**
     * TODO: Replace logic by {@see DsnParser}.
     *
     * Creates a connection by a DSN.
     */
    public static function fromDsn(string $dsn, EventLoopInterface $eventLoop): self
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

        parse_str($parsedUrl['query'] ?? '', $queryOptions);

        $clientOptions = array_replace($queryOptions, [
            'host'     => $parsedUrl['host'] ?? 'localhost',
            'port'     => $parsedUrl['port'] ?? $defaultPort,
            'vhost'    => isset($pathParts[0]) ? urldecode($pathParts[0]) : '/',
            'user'     => $parsedUrl['user'] ?? null,
            'password' => $parsedUrl['pass'] ?? null,
        ]);

        return new self($eventLoop, $clientOptions);
    }

    /**
     * @inheritDoc
     */
    public function isConsuming(string $queueName): bool
    {
        if (! isset($this->consumerStatusByQueueName[$queueName])) {
            return false;
        }

        return $this->consumerStatusByQueueName[$queueName] === self::CONSUMER_STATUS_PENDING
               || $this->consumerStatusByQueueName[$queueName] === self::CONSUMER_STATUS_RUNNING;
    }

    /**
     * @inheritDoc
     */
    public function consume(string $queueName, callable $onMessage): void
    {
        if ($this->isConsuming($queueName)) {
            throw new RuntimeException(sprintf('Consumer already exists for queue "%s".', $queueName));
        }

        $this->consumerStatusByQueueName[$queueName] = self::CONSUMER_STATUS_PENDING;

        $promise = $this->openAsyncChannel()->then(
            function (Channel $channel) use ($queueName, $onMessage) {
                $channelId = $this->getChannelId($channel);

                $channel->consume(
                    fn(Message $message) => $this->handleMessage($message, $channelId, $onMessage),
                    $queueName
                )->then(
                    function (MethodBasicConsumeOkFrame $frame) use ($channelId, $queueName) {
                        if ($this->logger !== null) {
                            $this->logger->debug(sprintf('Successfully started consumer for queue "%s".', $queueName));
                        }

                        $this->consumerTagsByChannelId[$channelId][] = $frame->consumerTag;
                        $this->consumerStatusByQueueName[$queueName] = self::CONSUMER_STATUS_RUNNING;
                    },
                    function () use ($queueName, $channelId) {
                        if ($this->logger !== null) {
                            $this->logger->error(sprintf('Failed starting consumer for queue "%s".', $queueName));
                        }

                        unset($this->consumerStatusByQueueName[$queueName]);
                        $this->closeChannel($channelId);
                    }
                );
            }
        );

        if ($promise instanceof ExtendedPromiseInterface) {
            $promise->done();
        }
    }

    /**
     * Handles a message that is received by a consumer.
     *
     * All this does is, it prevents a message from being handled further if the channel is closed (or being closed).
     *
     * @param Message                   $message
     * @param int                       $channelId
     * @param callable<Message, string> $callback
     */
    private function handleMessage(Message $message, int $channelId, callable $callback): void
    {
        if (! isset($this->channelById[$channelId])) {
            if ($this->logger !== null) {
                $this->logger->warning('Message received for closed channel. It will be automatically re-queued by AMQP.');
            }

            return;
        }

        $callback(new BunnyMessage($message, $channelId));
    }

    /**
     * @inheritDoc
     */
    public function cancelConsumers(): void
    {
        foreach (array_keys($this->consumerTagsByChannelId) as $channelId) {
            $this->closeChannel($channelId);
        }

        $this->consumerTagsByChannelId = [];
        $this->consumerStatusByQueueName = [];
    }

    /**
     * @inheritDoc
     */
    public function ack(MessageInterface $message): void
    {
        if (! $message instanceof BunnyMessage) {
            throw new LogicException('Unable to nack message. Message was not consumed by this connection.');
        }

        $promise = $this->getChannel($message->getChannelId())
            ->ack($message->getMessage());

        if ($promise instanceof ExtendedPromiseInterface) {
            // Throw exceptions when the promise has failed.
            $promise->done();
        }
    }

    /**
     * @inheritDoc
     */
    public function nack(MessageInterface $message, bool $requeue = false): void
    {
        if (! $message instanceof BunnyMessage) {
            throw new LogicException('Unable to nack message. Message was not consumed by this connection.');
        }

        $promise = $this->getChannel($message->getChannelId())
            ->nack($message->getMessage(), false, $requeue);

        if ($promise instanceof ExtendedPromiseInterface) {
            // Throw exceptions when the promise has failed.
            $promise->done();
        }
    }

    /**
     * @inheritDoc
     */
    public function publish(string $body, array $properties = [], ?string $exchangeName = null, ?string $routingKey = null): void
    {
        // This implementation requires the property names in kebab-case, so transform all properties to this convention.
        foreach ($properties as $name => $value) {
            $actualName = str_replace('_', '-', $name);
            if ($name !== $actualName) {
                $properties[$actualName] = $value;
                unset($properties[$name]);
            }
        }

        if (isset($properties['timestamp'])) {
            $properties['timestamp'] = (new DateTime())->setTimestamp($properties['timestamp']);
        }

        $this->getPublishChannel()->publish(
            $body,
            $properties,
            $exchangeName ?? '',
            $routingKey ?? ''
        );
    }

    /**
     * @inheritDoc
     */
    public function exchangeDeclare(
        string $name,
        string $type = 'direct',
        bool $passive = false,
        bool $durable = false,
        bool $autoDelete = false,
        bool $internal = false,
        array $arguments = []
    ): void {
        $this->getPublishChannel()->exchangeDeclare(
            $name,
            $type,
            $passive,
            $durable,
            $autoDelete,
            $internal,
            false,
            $arguments
        );
    }

    /**
     * @inheritDoc
     */
    public function exchangeBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void
    {
        $this->getPublishChannel()->exchangeBind(
            $exchangeName,
            $name,
            $routingKey,
            false,
            $arguments
        );
    }

    /**
     * @inheritDoc
     */
    public function queueDeclare(
        string $name,
        bool $passive = false,
        bool $durable = false,
        bool $exclusive = false,
        bool $autoDelete = false,
        array $arguments = []
    ): void {
        $this->getPublishChannel()->queueDeclare(
            $name,
            $passive,
            $durable,
            $exclusive,
            $autoDelete,
            false,
            $arguments
        );
    }

    /**
     * @inheritDoc
     */
    public function queueBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void
    {
        $this->getPublishChannel()->queueBind(
            $name,
            $exchangeName,
            $routingKey,
            false,
            $arguments
        );
    }

    /**
     * Get the channel to use for publish and declaration.
     */
    private function getPublishChannel(): Channel
    {
        if ($this->publishChannelId === null) {
            $channel = $this->openSyncChannel();
            $this->publishChannelId = $this->getChannelId($channel);
        }

        return $this->getChannel($this->publishChannelId);
    }

    /**
     * Get an existing channel by its id.
     *
     * @param int $channelId
     *
     * @return Channel
     *
     * @throws RuntimeException When the channel does not exist.
     */
    private function getChannel(int $channelId): Channel
    {
        if (! isset($this->channelById[$channelId])) {
            throw new RuntimeException(sprintf('Channel with id %s does not exist.', $channelId));
        }

        return $this->channelById[$channelId];
    }

    /**
     * Get the id of a channel.
     *
     * @param Channel $channel
     *
     * @return int
     *
     * @throws RuntimeException When the channel does not have an id.
     */
    private function getChannelId(Channel $channel): int
    {
        $objectId = spl_object_id($channel);

        if (! isset($this->idByChannel[$objectId])) {
            throw new RuntimeException('Channel does not have an id.');
        }

        return $this->idByChannel[$objectId];
    }

    /**
     * Register a channel. Assigns the channel an unique id.
     *
     * @param Channel $channel
     *
     * @throws RuntimeException When the channel is already registered.
     */
    private function registerChannel(Channel $channel): void
    {
        $objectId = spl_object_id($channel);

        if (isset($this->idByChannel[$objectId])) {
            throw new RuntimeException('Channel already registered.');
        }

        $channelId = self::$channelIdIncrement++;

        $this->channelById[$channelId] = $channel;
        $this->idByChannel[$objectId] = $channelId;
    }

    /**
     * Opens a new channel for async use.
     *
     * @return PromiseInterface<Channel>
     */
    private function openAsyncChannel(): PromiseInterface
    {
        return $this->connectAsyncIfNecessary()
            ->then(fn(AsyncClient $client) => $client->channel())
            ->then(function (Channel $channel) {
                $this->registerChannel($channel);

                return $channel;
            })
            ->then(
                fn(Channel $channel) => $channel
                    ->qos(0, 10) // TODO: Make configurable...
                    ->then(fn() => $channel)
            );
    }

    /**
     * Opens a new channel for sync use.
     */
    private function openSyncChannel(): Channel
    {
        if (! $this->syncClient->isConnected()) {
            $this->syncClient->connect();
        }

        $channel = $this->syncClient->channel();
        $this->registerChannel($channel);

        return $channel;
    }

    /**
     * Closes an open channel by its id.
     */
    private function closeChannel(int $channelId): void
    {
        $channel = $this->getChannel($channelId);

        unset($this->channelById[$channelId]);
        unset($this->idByChannel[spl_object_id($channel)]);

        $channel->close();
    }

    /**
     * @return PromiseInterface<AsyncClient>
     */
    private function connectAsyncIfNecessary(): PromiseInterface
    {
        if ($this->asyncClient->getState() === ClientStateEnum::CONNECTED) {
            return resolve($this->asyncClient);
        }

        if ($this->asyncClient->getState() === ClientStateEnum::CONNECTING) {
            return $this->asyncConnectionPromise;
        }

        return $this->asyncConnectionPromise = $this->asyncClient->connect()
            ->then(function () {
                $this->asyncConnectionPromise = null;

                return $this->asyncClient;
            });
    }
}
