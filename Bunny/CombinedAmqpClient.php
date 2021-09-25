<?php

declare(strict_types=1);

namespace Kurzyx\AsyncAmqpMessengerBundle\Bunny;

use Bunny\Client as BaseClient;
use Bunny\Async\Client as AsyncClient;
use React\EventLoop\LoopInterface as EventLoopInterface;

/**
 * @internal
 */
final class CombinedAmqpClient extends BaseClient
{
    private AsyncClient $client;

    public function __construct(EventLoopInterface $eventLoop, array $options = [])
    {
        $this->client = new AsyncClient($eventLoop, $options);

        parent::__construct($options);
    }

    protected function getStream()
    {
        return $this->client->getStream();
    }
}
