<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Connection;

use Exception;
use Kurzyx\AsyncAmqpMessengerBundle\DsnParser;
use Kurzyx\AsyncAmqpMessengerBundle\Message\AmqpLibMessage;
use Kurzyx\AsyncAmqpMessengerBundle\Message\MessageInterface;
use LogicException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Wire\IO\SocketIO;
use PhpAmqpLib\Wire\IO\StreamIO;
use React\EventLoop\LoopInterface as EventLoopInterface;
use RuntimeException;
use Throwable;

final class AmqpLibConnection implements ConnectionInterface
{
    private AbstractConnection $connection;
    private EventLoopInterface $eventLoop;

    private ?int $publishChannelId = null;
    private array $channelIdByQueueName = [];

    private bool $readStreamInitialized = false;

    public function __construct(AbstractConnection $connection, EventLoopInterface $eventLoop)
    {
        $this->connection = $connection;
        $this->eventLoop = $eventLoop;
    }

    /**
     * Creates a connection by a DSN.
     */
    public static function fromDsn(string $dsn, EventLoopInterface $eventLoop): self
    {
        $options = (new DsnParser())->parse($dsn);

        $connection = new AMQPLazyConnection(
            $options['host'],
            $options['port'],
            $options['user'],
            $options['password'],
            $options['vhost'],
            false,
            'AMQPLAIN',
            null,
            'en_US',
            isset($options['options']['connection_timeout']) ? (float) $options['options']['connection_timeout'] : 3.0,
            isset($options['options']['read_write_timeout']) ? (float) $options['options']['read_write_timeout'] : 3.0,
            null,
            false,
            isset($options['options']['heartbeat']) ? (int) $options['options']['heartbeat'] : 0,
            isset($options['options']['channel_rpc_timeout']) ? (float) $options['options']['channel_rpc_timeout'] : 0.0,
            null
        );

        return new self($connection, $eventLoop);
    }

    /**
     * @inheritDoc
     */
    public function isConsuming(string $queueName): bool
    {
        return isset($this->channelIdByQueueName[$queueName]);
    }

    /**
     * @inheritDoc
     *
     * @throws Throwable
     */
    public function consume(string $queueName, callable $onMessage): void
    {
        if ($this->isConsuming($queueName)) {
            throw new RuntimeException(sprintf('Consumer already exists for queue "%s".', $queueName));
        }

        $handleMessage = static function (AMQPMessage $message) use ($onMessage) {
            $onMessage(new AmqpLibMessage($message));
        };

        $channel = $this->getConsumerChannel();

        try {
            $channel->basic_consume($queueName, '', false, false, false, false, $handleMessage);
        } catch (Throwable $throwable) {
            $channel->close();

            throw $throwable;
        }

        $this->channelIdByQueueName[$queueName] = $channel->getChannelId();
    }

    /**
     * @inheritDoc
     */
    public function cancelConsumers(): void
    {
        foreach ($this->channelIdByQueueName as $channelId) {
            $this->getConsumerChannel($channelId)->close();
        }

        $this->channelIdByQueueName = [];
    }

    /**
     * @inheritDoc
     */
    public function ack(MessageInterface $message): void
    {
        if (! $message instanceof AmqpLibMessage || $message->getMessage()->getChannel()->getConnection() !== $this->connection) {
            throw new LogicException('Unable to ack message. Message was not consumed by this connection.');
        }

        $message->getMessage()->ack();
    }

    /**
     * @inheritDoc
     */
    public function nack(MessageInterface $message, bool $requeue = false): void
    {
        if (! $message instanceof AmqpLibMessage || $message->getMessage()->getChannel()->getConnection() !== $this->connection) {
            throw new LogicException('Unable to nack message. Message was not consumed by this connection.');
        }

        $message->getMessage()->nack($requeue);
    }

    /**
     * @inheritDoc
     */
    public function publish(string $body, array $properties = [], ?string $exchangeName = null, ?string $routingKey = null): void
    {
        $message = new AMQPMessage($body);

        if (isset($properties['headers'])) {
            $properties['application_headers'] = new AMQPTable($properties['headers']);
            unset($properties['headers']);
        }

        foreach ($properties as $name => $value) {
            $message->set($name, $value);
        }

        $this->getPublishChannel()->basic_publish($message, $exchangeName ?? '', $routingKey ?? '');
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
        $this->getPublishChannel()->exchange_declare(
            $name,
            $type,
            $passive,
            $durable,
            $autoDelete,
            $internal,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * @inheritDoc
     */
    public function exchangeBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void
    {
        $this->getPublishChannel()->exchange_bind(
            $exchangeName,
            $name,
            $routingKey,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * @inheritDoc
     */
    public function queueDeclare(string $name, bool $passive = false, bool $durable = false, bool $exclusive = false, bool $autoDelete = false, array $arguments = []): void
    {
        $this->getPublishChannel()->queue_declare(
            $name,
            $passive,
            $durable,
            $exclusive,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * @inheritDoc
     */
    public function queueBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void
    {
        $this->getPublishChannel()->queue_bind(
            $name,
            $exchangeName,
            $routingKey,
            false,
            new AMQPTable($arguments)
        );
    }

    private function getPublishChannel(): AMQPChannel
    {
        if ($this->publishChannelId === null) {
            $this->publishChannelId = $this->connection->channel()->getChannelId();
        }

        return $this->connection->channel($this->publishChannelId);
    }

    private function getConsumerChannel(?int $channelId = null): AMQPChannel
    {
        $channel = $this->connection->channel($channelId);

        if (! $this->readStreamInitialized) {
            $io = $this->connection->getIO();

            if (! $io instanceof StreamIO && ! $io instanceof SocketIO) {
                throw new LogicException('Connection returned an unsupported IO.');
            }

            try {
                $this->eventLoop->addReadStream(
                    $io->getSocket(),
                    fn() => $this->onDataAvailable()
                );
            } catch (Exception $exception) {
                throw new RuntimeException('Failed to bind IO to event-loop.', 0, $exception);
            }

            $this->readStreamInitialized = true;
        }

        // ...
        $channel->basic_qos(0, 10, false);

        return $channel;
    }

    private function onDataAvailable(): void
    {
        foreach ($this->channelIdByQueueName as $channelId) {
            $channel = $this->getConsumerChannel($channelId);

            do {
                $channel->wait(null, true);
            } while ($channel->hasPendingMethods());
        }

        // It is possible that data is read outside the event-loop (addReadStream callback). For example, this happens
        // when a queue or exchange declaration is done, or when a channel/consumer is opened. It could happen that at
        // this moment, a new message read by the customer. Since this doesn't happen within the while loop above, this
        // message will not be processed until there's more data available (and when the event-loop is running).
        // To prevent this scenario (which is very common), always check whether there's data available when the
        // event-loop is started.
        $this->eventLoop->futureTick(function () {
            $this->checkForPendingData();
        });
    }

    private function checkForPendingData(): void
    {
        foreach ($this->channelIdByQueueName as $channelId) {
            if ($this->getConsumerChannel($channelId)->hasPendingMethods()) {
                $this->onDataAvailable();

                break;
            }
        }
    }
}
