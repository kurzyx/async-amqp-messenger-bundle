<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Message stamp that allows for specifying how the AMQP message is routed.
 */
final class AmqpRoutingStamp implements NonSendableStampInterface
{
    private ?string $routingKey;
    private ?string $exchangeName;

    public function __construct(?string $routingKey = null, ?string $exchangeName = null)
    {
        $this->routingKey = $routingKey;
        $this->exchangeName = $exchangeName;
    }

    public function getRoutingKey(): ?string
    {
        return $this->routingKey;
    }

    public function getExchangeName(): ?string
    {
        return $this->exchangeName;
    }
}
