<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Kurzyx\AsyncAmqpMessengerBundle\Message\MessageInterface;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Message stamp that contains the original AMQP message and queue name it was consumed from.
 */
final class AmqpReceivedStamp implements NonSendableStampInterface
{
    private MessageInterface $message;
    private string $queueName;

    public function __construct(MessageInterface $message, string $queueName)
    {
        $this->message = $message;
        $this->queueName = $queueName;
    }

    public function getMessage(): MessageInterface
    {
        return $this->message;
    }

    public function getQueueName(): string
    {
        return $this->queueName;
    }
}
