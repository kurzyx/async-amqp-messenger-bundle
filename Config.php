<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

/**
 * @internal
 */
final class Config
{
    private const DEFAULT_EXCHANGE_NAME            = 'messages';
    private const DEFAULT_ROUTING_KEY              = '';
    private const DEFAULT_DELAY_QUEUE_NAME_PATTERN = 'delay_%exchange_name%_%routing_key%_%delay%';
    private const DEFAULT_DELAY_EXCHANGE_NAME      = 'delays';

    private array $options;

    public function __construct(array $options)
    {
        $this->options = $options;
    }

    public function shouldAutoSetup(): bool
    {
        return $this->options['auto_setup'] ?? true;
    }

    public function getDefaultExchangeName(): string
    {
        return $this->options['default_exchange_name'] ?? self::DEFAULT_EXCHANGE_NAME;
    }

    public function getDefaultRoutingKey(): string
    {
        return $this->options['default_routing_key'] ?? self::DEFAULT_ROUTING_KEY;
    }

    public function getDelayQueueNamePattern(): string
    {
        return $this->options['delay']['queue_name_pattern'] ?? self::DEFAULT_DELAY_QUEUE_NAME_PATTERN;
    }

    public function getDelayExchangeName(): string
    {
        return $this->options['delay']['exchange_name'] ?? self::DEFAULT_DELAY_EXCHANGE_NAME;
    }

    public function getExchanges(): array
    {
        return $this->options['exchanges'] ?? [];
    }

    public function getExchangeBindings(): array
    {
        return $this->options['exchange_bindings'] ?? [];
    }

    public function getQueues(): array
    {
        return $this->options['queues'] ?? [];
    }

    public function getQueueBindings(): array
    {
        return $this->options['queue_bindings'] ?? [];
    }
}
