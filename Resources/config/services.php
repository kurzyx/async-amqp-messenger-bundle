<?php

declare(strict_types=1);

use Kurzyx\AsyncAmqpMessengerBundle\AsyncAmqpTransportFactory;
use Kurzyx\AsyncAmqpMessengerBundle\RejectRedeliveredMessageMiddleware;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container) {
    $container->services()
        ->set('kurzyx_async_amqp_messenger.reject_redelivered_message_middleware', RejectRedeliveredMessageMiddleware::class)
            ->decorate('messenger.middleware.reject_redelivered_message_middleware')
            ->args([service('kurzyx_async_amqp_messenger.reject_redelivered_message_middleware.inner')])
        ->set('kurzyx_async_amqp_messenger.transport_factory', AsyncAmqpTransportFactory::class)
            ->args([service('kurzyx_async_messenger.event_loop')])
            ->call('setLogger', [service('logger')])
            ->tag('messenger.transport_factory')
            ->tag('kernel.reset', ['method' => 'reset'])
    ;
};
