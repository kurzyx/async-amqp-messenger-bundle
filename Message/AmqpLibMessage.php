<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Message;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

final class AmqpLibMessage implements MessageInterface
{
    private AMQPMessage $message;

    public function __construct(AMQPMessage $message)
    {
        $this->message = $message;
    }

    /**
     * @return AMQPMessage
     */
    public function getMessage(): AMQPMessage
    {
        return $this->message;
    }

    public function getContent(): string
    {
        return $this->message->getBody();
    }

    public function getHeaders(): array
    {
        /** @var AMQPTable $headers */
        $headers = $this->message->get('application_headers');

        return $headers->getNativeData();
    }

    public function isRedelivered(): bool
    {
        return $this->message->get('redelivered');
    }
}
