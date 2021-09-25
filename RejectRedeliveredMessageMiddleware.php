<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\RejectRedeliveredMessageException;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;

/**
 * Middleware that throws a RejectRedeliveredMessageException when a message is detected that has been redelivered by AMQP.
 *
 * @see \Symfony\Component\Messenger\Middleware\RejectRedeliveredMessageMiddleware
 */
final class RejectRedeliveredMessageMiddleware implements MiddlewareInterface
{
    private MiddlewareInterface $middleware;

    public function __construct(MiddlewareInterface $middleware)
    {
        $this->middleware = $middleware;
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        /** @var AmqpReceivedStamp|null $amqpReceivedStamp */
        $amqpReceivedStamp = $envelope->last(AmqpReceivedStamp::class);
        if ($amqpReceivedStamp !== null && $amqpReceivedStamp->getMessage()->redelivered) {
            throw new RejectRedeliveredMessageException('Redelivered message from AMQP detected that will be rejected and trigger the retry logic.');
        }

        return $this->middleware->handle($envelope, $stack);
    }
}
