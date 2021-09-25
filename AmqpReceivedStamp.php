<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Bunny\Message;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Message stamp that contains the original AMQP message and queue name it was consumed from. At last, it contains the
 * channel-id it was consumed through, however this is only useful for the transport.
 */
final class AmqpReceivedStamp implements NonSendableStampInterface
{
    private Message $message;
    private string $queueName;
    private int $channelId;

    public function __construct(Message $message, string $queueName, int $channelId)
    {
        $this->message = $message;
        $this->queueName = $queueName;
        $this->channelId = $channelId;
    }

    public function getMessage(): Message
    {
        return $this->message;
    }

    public function getQueueName(): string
    {
        return $this->queueName;
    }

    /**
     * @internal
     */
    public function getChannelId(): int
    {
        return $this->channelId;
    }
}
