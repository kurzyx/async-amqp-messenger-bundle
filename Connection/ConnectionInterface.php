<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Connection;

use Kurzyx\AsyncAmqpMessengerBundle\Message\MessageInterface;
use RuntimeException;

interface ConnectionInterface
{
    /**
     * Whether a consumer is running (or pending) for the given queue.
     */
    public function isConsuming(string $queueName): bool;

    /**
     * Starts a consumer for a queue.
     *
     * Note that only a single consumer can be started for each queue.
     *
     * This method works asynchronously.
     *
     * @param string                    $queueName
     * @param callable<MessageInterface> $onMessage
     *
     * @throws RuntimeException When the queue is already being consumed.
     */
    public function consume(string $queueName, callable $onMessage): void;

    /**
     * Cancels all running consumers.
     *
     * This method works asynchronously.
     */
    public function cancelConsumers(): void;

    /**
     * Acknowledge a message.
     *
     * This method works asynchronously.
     */
    public function ack(MessageInterface $message): void;

    /**
     * Not acknowledge (reject) a message.
     *
     * This method works asynchronously.
     */
    public function nack(MessageInterface $message, bool $requeue = false): void;

    /**
     * Publishes a message.
     *
     * This method works synchronously.
     */
    public function publish(string $body, array $properties = [], ?string $exchangeName = null, ?string $routingKey = null): void;

    /**
     * Calls exchange.declare AMQP method.
     *
     * This method works synchronously.
     */
    public function exchangeDeclare(
        string $name,
        string $type = 'direct',
        bool $passive = false,
        bool $durable = false,
        bool $autoDelete = false,
        bool $internal = false,
        array $arguments = []
    ): void;

    /**
     * Calls exchange.bind AMQP method.
     *
     * This method works synchronously.
     */
    public function exchangeBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void;

    /**
     * Calls queue.declare AMQP method.
     *
     * This method works synchronously.
     */
    public function queueDeclare(string $name, bool $passive = false, bool $durable = false, bool $exclusive = false, bool $autoDelete = false, array $arguments = []): void;

    /**
     * Calls queue.bind AMQP method.
     *
     * This method works synchronously.
     */
    public function queueBind(string $name, string $exchangeName, string $routingKey = '', array $arguments = []): void;
}
