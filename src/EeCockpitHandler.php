<?php

declare(strict_types = 1);

namespace EventEngine\EeCockpit;

use EventEngine\Aggregate\AggregateEventEnvelope;
use EventEngine\DocumentStore\DocumentStore;
use EventEngine\DocumentStore\Filter\AnyFilter;
use EventEngine\EventEngine;
use Laminas\Diactoros\Response\JsonResponse;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\RequestHandlerInterface;

final class EeCockpitHandler implements RequestHandlerInterface
{
    /**
     * @var EventEngine
     */
    private $eventEngine;

    /**
     * @var DocumentStore
     */
    private $documentStore;

    public function __construct(EventEngine $eventEngine, DocumentStore $documentStore)
    {
        $this->eventEngine = $eventEngine;
        $this->documentStore = $documentStore;
    }

    public function handle(ServerRequestInterface $request): ResponseInterface
    {
        $urlParts = \explode('/', $request->getUri()->getPath());

        switch ($urlParts[\count($urlParts) - 1]) {
            case 'schema': return $this->handleLoadSchema($request);
            case 'load-aggregates': return $this->handleLoadAggregates($request);
            case 'load-aggregate-events': return $this->handleLoadAggregateEvents($request);
            case 'load-aggregate': return $this->handleLoadAggregate($request);
        }

        throw new \RuntimeException(
            "Could not resolve {$urlParts[\count($urlParts)]} to a handler. " .
            'Please make sure that you are using a compatible version of the ee-cockpit-php-backend.'
        );
    }

    /**
     * Compiles the schema for the event-engine-cockpit.
     *
     * @param ServerRequestInterface $request
     * @return ResponseInterface
     */
    private function handleLoadSchema(ServerRequestInterface $request): ResponseInterface
    {
        $config = $this->eventEngine->compileCacheableConfig();
        $messageBoxSchema = $this->eventEngine->messageBoxSchema();

        $array_map_key = function(callable $func, array $array) {
            $newArray = [];

            foreach ($array as $key => $value) {
                $newArray[] = $func($key, $value);
            }

            return $newArray;
        };

        $responseData = [
            'aggregates' => [],
            'queries' => $array_map_key(
                function(string $queryName, array $queryPayloadSchema) {
                    return ['queryName' => $queryName, 'schema' => $queryPayloadSchema];
                },
                $config['queryMap']
            ),
            'commands' => $array_map_key(
                function(string $commandName, array $commandPayloadSchema) use($config) {
                    return [
                        'commandName' => $commandName,
                        'schema' => $commandPayloadSchema,
                        'aggregateType' => $config['compiledCommandRouting'][$commandName]['aggregateType'] ?? null,
                        'createAggregate' => $config['compiledCommandRouting'][$commandName]['createAggregate'] ?? false
                    ];
                },
                $config['commandMap']
            ),
            'definitions' => $messageBoxSchema['definitions']
        ];

        foreach($config['aggregateDescriptions'] as $aggregateName => $aggregateConfig) {
            $responseData['aggregates'][] = [
                'aggregateType' => $aggregateConfig['aggregateType'],
                'aggregateIdentifier' => $aggregateConfig['aggregateIdentifier'],
                'aggregateStream' => $aggregateConfig['aggregateStream'],
                'aggregateCollection' => $aggregateConfig['aggregateCollection'],
                'multiStoreMode' => $aggregateConfig['multiStoreMode'],
                'commands' => $array_map_key(
                    function(string $commandName, array $commandRouting) use($config) {
                        $commandSchema = $config['commandMap'][$commandName];

                        return [
                            'commandName' => $commandName,
                            'aggregateType' => $commandRouting['aggregateType'],
                            'createAggregate' => $commandRouting['createAggregate'],
                            'schema' => $commandSchema
                        ];
                    },
                    \array_filter($config['compiledCommandRouting'], function($commandRouting) use($aggregateConfig) {
                        return $aggregateConfig['aggregateType'] === $commandRouting['aggregateType'];
                    })
                ),
                'events' => \array_values(\array_reduce(
                    \array_map(
                        function(array $commandRouting) use($config) {
                            $eventNames = \array_keys($commandRouting['eventRecorderMap']);
                            return $eventNames;
                        },
                        \array_filter($config['compiledCommandRouting'], function($commandRouting) use($aggregateConfig) {
                            return $aggregateConfig['aggregateType'] === $commandRouting['aggregateType'];
                        })
                    ),
                    function (array $carry, array $eventNames) use($config) {
                        foreach ($eventNames as $eventName) {
                            if (!isset($carry[$eventName])) {
                                $eventSchema = $config['eventMap'][$eventName];

                                $carry[$eventName] = [
                                    'eventName' => $eventName,
                                    'schema' => $eventSchema
                                ];
                            }
                        }

                        return $carry;
                    },
                    []
                ))
            ];
        }

        return new JsonResponse($responseData);
    }

    /**
     * Loads a list of aggregates for the given aggregate type from the persisted state in the document store.
     *
     * @param ServerRequestInterface $request
     * @return ResponseInterface
     */
    private function handleLoadAggregates(ServerRequestInterface $request): ResponseInterface
    {
        $aggregateType = $request->getQueryParams()['aggregateType'];
        $limit = (int) $request->getQueryParams()['limit'];
        $config = $this->eventEngine->compileCacheableConfig();

        if (!isset($config['aggregateDescriptions'][$aggregateType])) {
            throw new \RuntimeException('Unknown aggregate type ' . $aggregateType);
        }

        $aggregateCollection = $config['aggregateDescriptions'][$aggregateType]['aggregateCollection'];

        $documents = \iterator_to_array($this->documentStore->filterDocs($aggregateCollection, new AnyFilter(), 0, $limit));
        return new JsonResponse($documents);
    }

    /**
     * Loads a specific aggregate by replaying all or some of its events.
     *
     * @param ServerRequestInterface $request
     * @return ResponseInterface
     */
    private function handleLoadAggregate(ServerRequestInterface $request): ResponseInterface
    {
        $aggregateType = $request->getQueryParams()['aggregateType'];
        $aggregateId = $request->getQueryParams()['aggregateId'];
        $version = $request->getQueryParams()['version'] ?? null;
        $config = $this->eventEngine->compileCacheableConfig();

        if (!isset($config['aggregateDescriptions'][$aggregateType])) {
            throw new \RuntimeException('Unknown aggregate type ' . $aggregateType);
        }

        if (null === $version) {
            $state = $this->eventEngine->loadAggregateState($aggregateType, $aggregateId);
        } else {
            $state = $this->eventEngine->loadAggregateStateUntil($aggregateType, $aggregateId, (int) $version);
        }

        return new JsonResponse($state->toArray());
    }

    /**
     * Loads all events for a given aggregate.
     *
     * @param ServerRequestInterface $request
     * @return ResponseInterface
     */
    private function handleLoadAggregateEvents(ServerRequestInterface $request): ResponseInterface
    {
        $aggregateType = $request->getQueryParams()['aggregateType'];
        $aggregateId = $request->getQueryParams()['aggregateId'];
        $config = $this->eventEngine->compileCacheableConfig();

        if (!isset($config['aggregateDescriptions'][$aggregateType])) {
            throw new \RuntimeException('Unknown aggregate type ' . $aggregateType);
        }

        $events = [];

        /** @var AggregateEventEnvelope $event */
        foreach ($this->eventEngine->loadAggregateEvents($aggregateType, $aggregateId) as $event) {
            $events[] = [
                'eventName' => $event->eventName(),
                'aggregateVersion' => $event->aggregateVersion(),
                'createdAt' => $event->createdAt()->format(\DateTime::ATOM),
                'metadata' => $event->metadata(),
                'payload' => $event->rawPayload()
            ];
        }

        return new JsonResponse($events);
    }
}
