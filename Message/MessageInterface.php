<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Message;

interface MessageInterface
{
    public function getContent(): string;

    public function getHeaders(): array;

    public function isRedelivered(): bool;
}
