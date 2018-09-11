<?php
namespace Flowpack\JobQueue\Redis\Queue;

/*
 * This file is part of the Flowpack.JobQueue.Redis package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Utility\Algorithms;
use Flowpack\JobQueue\Common\Exception as JobQueueException;

/**
 * A queue implementation using Redis as the queue backend
 *
 * Depends on the php-redis extension.
 */
class RedisQueue implements QueueInterface
{

    /**
     * @var string
     */
    protected $name;

    /**
     * @var \Redis
     */
    protected $client;

    /**
     * @var integer
     */
    protected $defaultTimeout = 30;

    /**
     * @var array
     */
    protected $clientOptions;

    /**
     * @var float
     */
    protected $reconnectDelay = 1.0;

    /**
     * @var float
     */
    protected $reconnectDecay = 1.5;

    /**
     * @var float
     */
    protected $maxReconnectDelay = 30.0;

    /**
     * @param string $name
     * @param array $options
     * @throws JobQueueException
     */
    public function __construct($name, array $options = [])
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $this->clientOptions = isset($options['client']) ? $options['client'] : [];
        $this->client = new \Redis();
        if (!$this->connectClient()) {
            throw new JobQueueException('Could not connect to Redis', 1467382685);
        }
    }

    /**
     * @inheritdoc
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function submit($payload, array $options = []): string
    {
        $this->checkClientConnection();
        $messageId = Algorithms::generateUUID();
        $idStored = $this->client->hSet("queue:{$this->name}:ids", $messageId, json_encode($payload));
        if ($idStored === 0) {
            throw new JobQueueException(sprintf('Duplicate message id: "%s"', $messageId), 1470656350);
        }

        $this->client->lPush("queue:{$this->name}:messages", $messageId);
        return $messageId;
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function waitAndTake(?int $timeout = null): ?Message
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->checkClientConnection();
        $keyAndValue = $this->client->brPop("queue:{$this->name}:messages", $timeout);
        $messageId = isset($keyAndValue[1]) ? $keyAndValue[1] : null;
        if ($messageId === null) {
            return null;
        }
        $message = $this->getMessageById($messageId);
        if ($message !== null) {
            $this->client->hDel("queue:{$this->name}:ids", $messageId);
        }
        return $message;
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function waitAndReserve(?int $timeout = null): ?Message
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->checkClientConnection();
        $messageId = $this->client->brpoplpush("queue:{$this->name}:messages", "queue:{$this->name}:processing", $timeout);
        return $this->getMessageById($messageId);
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function release(string $messageId, array $options = []): void
    {
        $this->checkClientConnection();
        $this->client->multi()
            ->lRem("queue:{$this->name}:processing", $messageId, 0)
            ->hIncrBy("queue:{$this->name}:releases", $messageId, 1)
            ->lPush("queue:{$this->name}:messages", $messageId)
            ->exec();
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function abort(string $messageId): void
    {
        $this->checkClientConnection();
        $numberOfRemoved = $this->client->lRem("queue:{$this->name}:processing", $messageId, 0);
        if ($numberOfRemoved === 1) {
            $this->client->lPush("queue:{$this->name}:failed", $messageId);
        }
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function finish(string $messageId): bool
    {
        $this->checkClientConnection();
        $numberOfRemoved = $this->client->lRem("queue:{$this->name}:processing", $messageId, 0);
        $this->client->hDel("queue:{$this->name}:ids", $messageId);
        $this->client->hDel("queue:{$this->name}:releases", $messageId);
        return $numberOfRemoved > 0;
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function peek(int $limit = 1): array
    {
        $this->checkClientConnection();
        $result = $this->client->lRange("queue:{$this->name}:messages", -($limit), -1);
        if (!is_array($result) || count($result) === 0) {
            return [];
        }
        $messages = [];
        foreach ($result as $messageId) {
            $encodedPayload = $this->client->hGet("queue:{$this->name}:ids", $messageId);
            $messages[] = new Message($messageId, json_decode($encodedPayload, true));
        }
        return $messages;
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function countReady(): int
    {
        $this->checkClientConnection();
        return $this->client->lLen("queue:{$this->name}:messages");
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function countReserved(): int
    {
        $this->checkClientConnection();
        return $this->client->lLen("queue:{$this->name}:processing");
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function countFailed(): int
    {
        $this->checkClientConnection();
        return $this->client->lLen("queue:{$this->name}:failed");
    }

    /**
     * @return void
     * @throws JobQueueException
     */
    public function setUp(): void
    {
        $this->checkClientConnection();
    }

    /**
     * @inheritdoc
     * @throws JobQueueException
     */
    public function flush(): void
    {
        $this->checkClientConnection();
        $this->client->flushDB();
    }

    /**
     * @param string $messageId
     * @return Message
     */
    protected function getMessageById(string $messageId): ?Message
    {
        if (!is_string($messageId)) {
            return null;
        }
        $encodedPayload = $this->client->hGet("queue:{$this->name}:ids", $messageId);
        $numberOfReleases = (integer)$this->client->hGet("queue:{$this->name}:releases", $messageId);
        return new Message($messageId, json_decode($encodedPayload, true), $numberOfReleases);
    }

    /**
     * Check if the Redis client connection is still up and reconnect if Redis was disconnected
     *
     * @return void
     * @throws JobQueueException
     */
    protected function checkClientConnection()
    {
        $reconnect = false;
        try {
            $pong = $this->client->ping();
            if ($pong === false) {
                $reconnect = true;
            }
        } catch (\RedisException $e) {
            $reconnect = true;
        }
        if ($reconnect) {
            if (!$this->connectClient()) {
                throw new JobQueueException('Could not connect to Redis', 1467382685);
            }
        }
    }

    /**
     * Connect the Redis client
     *
     * Will back off if the connection could not be established (e.g. Redis server gone away / restarted) until max
     * reconnect delay is achieved. This prevents busy waiting for the Redis connection when used from a job worker.
     *
     * @return bool True if the client could connect to Redis
     */
    protected function connectClient()
    {
        $host = isset($this->clientOptions['host']) ? $this->clientOptions['host'] : '127.0.0.1';
        $port = isset($this->clientOptions['port']) ? $this->clientOptions['port'] : 6379;
        $password = isset($this->clientOptions['password']) ? $this->clientOptions['password'] : '';
        $database = isset($this->clientOptions['database']) ? $this->clientOptions['database'] : 0;
        // The connection read timeout should be higher than the timeout for blocking operations!
        $timeout = isset($this->clientOptions['timeout']) ? $this->clientOptions['timeout'] : round($this->defaultTimeout * 1.5);

        $connected = $this->client->connect($host, $port, $timeout);
        if ($connected) {
            if($password !== '') {
                if (!$this->client->auth($password)) {
                    throw new JobQueueException('Redis authentication failed.', 1502366245);
                }
            }
            $connected = $this->client->select($database);
        }

        // Break the cycle that could cause a high CPU load
        if (!$connected) {
            usleep($this->reconnectDelay * 1e6);
            $this->reconnectDelay = min($this->reconnectDelay * $this->reconnectDecay, $this->maxReconnectDelay);
        } else {
            $this->reconnectDelay = 1.0;
        }
        return $connected;
    }
}
