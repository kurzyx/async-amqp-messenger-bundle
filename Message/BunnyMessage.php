<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Message;

use Bunny\Message;

final class BunnyMessage implements MessageInterface
{
    private Message $message;
    private int $channelId;

    public function __construct(Message $message, int $channelId)
    {
        $this->message = $message;
        $this->channelId = $channelId;
    }

    public function getMessage(): Message
    {
        return $this->message;
    }

    public function getChannelId(): int
    {
        return $this->channelId;
    }

    public function getContent(): string
    {
        return $this->message->content;
    }

    public function getHeaders(): array
    {
        return $this->message->headers;
    }

    public function isRedelivered(): bool
    {
        return $this->message->redelivered;
    }
}
