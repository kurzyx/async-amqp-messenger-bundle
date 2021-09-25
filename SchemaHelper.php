<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

/**
 * @internal
 */
final class SchemaHelper
{
    private Connection $connection;
    private Config $config;
    private bool $alreadySetup = false;

    public function __construct(Connection $connection, Config $config)
    {
        $this->connection = $connection;
        $this->config = $config;
    }

    public function setup(): void
    {
        if ($this->alreadySetup) {
            return;
        }

        $this->connection->exchangeDeclare(
            $this->config->getDelayExchangeName(),
            'direct',
            false,
            true,
        );

        $this->declareExchanges();
        $this->bindExchanges();

        $this->declareQueues();
        $this->bindQueues();

        $this->alreadySetup = true;
    }

    private function declareExchanges(): void
    {
        foreach ($this->config->getExchanges() as $name => $exchangeOptions) {
            $this->connection->exchangeDeclare(
                $name,
                $exchangeOptions['type'] ?? 'direct',
                $exchangeOptions['passive'] ?? false,
                $exchangeOptions['durable'] ?? true,
                $exchangeOptions['autoDelete'] ?? false,
                $exchangeOptions['internal'] ?? false,
                $exchangeOptions['arguments'] ?? [],
            );
        }
    }

    private function bindExchanges(): void
    {
        foreach ($this->config->getExchangeBindings() as $bindingOptions) {
            $this->connection->exchangeBind(
                $bindingOptions['name'],
                $bindingOptions['exchange_name'],
                $bindingOptions['routing_key'] ?? '',
                $bindingOptions['arguments'] ?? [],
            );
        }
    }

    private function declareQueues(): void
    {
        foreach ($this->config->getQueues() as $name => $queueOptions) {
            $this->connection->queueDeclare(
                $name,
                $queueOptions['passive'] ?? false,
                $queueOptions['durable'] ?? true,
                $queueOptions['exclusive'] ?? false,
                $queueOptions['autoDelete'] ?? false,
                $queueOptions['arguments'] ?? [],
            );
        }
    }

    private function bindQueues(): void
    {
        foreach ($this->config->getQueueBindings() as $bindingOptions) {
            $this->connection->queueBind(
                $bindingOptions['name'],
                $bindingOptions['exchange_name'],
                $bindingOptions['routing_key'] ?? '',
                $bindingOptions['arguments'] ?? [],
            );
        }
    }
}
