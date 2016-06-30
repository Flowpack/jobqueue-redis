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
use TYPO3\Flow\Utility\Algorithms;

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
    protected $defaultTimeout = 60;

    /**
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options = [])
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $clientOptions = isset($options['client']) ? $options['client'] : [];
        $host = isset($clientOptions['host']) ? $clientOptions['host'] : '127.0.0.1';
        $port = isset($clientOptions['port']) ? $clientOptions['port'] : 6379;
        $database = isset($clientOptions['database']) ? $clientOptions['database'] : 0;

        $this->client = new \Redis();
        $this->client->connect($host, $port);
        $this->client->select($database);
    }

    /**
     * @inheritdoc
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     */
    public function submit($payload, array $options = [])
    {
        $messageId = Algorithms::generateUUID();
        $idStored = $this->client->hSet("queue:{$this->name}:ids", $messageId, json_encode($payload));
        if ($idStored === 0) {
            return null;
        }

        $this->client->lPush("queue:{$this->name}:messages", $messageId);
        return $messageId;
    }

    /**
     * @inheritdoc
     */
    public function waitAndTake($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
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
     */
    public function waitAndReserve($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $messageId = $this->client->brpoplpush("queue:{$this->name}:messages", "queue:{$this->name}:processing", $timeout);
        return $this->getMessageById($messageId);
    }

    /**
     * @inheritdoc
     */
    public function release($messageId, array $options = [])
    {
        $this->client->lRem("queue:{$this->name}:processing", $messageId, 0);
        $numberOfFailures = (integer)$this->client->hGet("queue:{$this->name}:failures", $messageId);
        $this->client->hSet("queue:{$this->name}:failures", $messageId, $numberOfFailures + 1);
        $this->client->lPush("queue:{$this->name}:messages", $messageId);
    }

    /**
     * @inheritdoc
     */
    public function abort($messageId)
    {
        $numberOfRemoved = $this->client->lRem("queue:{$this->name}:processing", $messageId, 0);
        if ($numberOfRemoved === 1) {
            $this->client->lPush("queue:{$this->name}:failed", $messageId);
        }
    }

    /**
     * @inheritdoc
     */
    public function finish($messageId)
    {
        $this->client->hDel("queue:{$this->name}:ids", $messageId);
        $this->client->hDel("queue:{$this->name}:failures", $messageId);
        return $this->client->lRem("queue:{$this->name}:processing", $messageId, 0) > 0;
    }

    /**
     * @inheritdoc
     */
    public function peek($limit = 1)
    {
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
     */
    public function count()
    {
        return $this->client->lLen("queue:{$this->name}:messages");
    }

    /**
     * @return void
     */
    public function setUp()
    {
        // TODO
    }

    /**
     * @inheritdoc
     */
    public function flush()
    {
        $this->client->flushDB();
    }

    /**
     * @param string $messageId
     * @return Message
     */
    protected function getMessageById($messageId)
    {
        if (!is_string($messageId)) {
            return null;
        }
        $encodedPayload = $this->client->hGet("queue:{$this->name}:ids", $messageId);
        $numberOfFailures = (integer)$this->client->hGet("queue:{$this->name}:failures", $messageId);
        return new Message($messageId, json_decode($encodedPayload, true), $numberOfFailures);
    }
}