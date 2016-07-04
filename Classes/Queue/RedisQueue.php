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
     * Constructor
     *
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options = array())
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $this->clientOptions = isset($options['client']) ? $options['client'] : array();

        $this->client = new \Redis();
        if (!$this->connectClient()) {
            throw new \Flowpack\JobQueue\Common\Exception('Could not connect to Redis', 1467382685);
        }
    }

    /**
     * Submit a message to the queue
     *
     * @param Message $message
     * @return void
     */
    public function submit(Message $message)
    {
        $this->checkClientConnection();
        if ($message->getIdentifier() !== null) {
            $added = $this->client->sAdd("queue:{$this->name}:ids", $message->getIdentifier());
            if (!$added) {
                return;
            }
        }
        $encodedMessage = $this->encodeMessage($message);
        $this->client->lPush("queue:{$this->name}:messages", $encodedMessage);
        $message->setState(Message::STATE_SUBMITTED);
    }

    /**
     * Wait for a message in the queue and return the message for processing
     * (without safety queue)
     *
     * @param int $timeout
     * @return Message The received message or NULL if a timeout occurred
     */
    public function waitAndTake($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->checkClientConnection();
        $keyAndValue = $this->client->brPop("queue:{$this->name}:messages", $timeout);
        $value = isset($keyAndValue[1]) ? $keyAndValue[1] : null;
        if (is_string($value)) {
            $message = $this->decodeMessage($value);

            if ($message->getIdentifier() !== null) {
                $this->client->sRem("queue:{$this->name}:ids", $message->getIdentifier());
            }

            // The message is marked as done
            $message->setState(Message::STATE_DONE);

            return $message;
        } else {
            return null;
        }
    }

    /**
     * Wait for a message in the queue and save the message to a safety queue
     *
     * TODO: Idea for implementing a TTR (time to run) with monitoring of safety queue. E.g.
     * use different queue names with encoded times? With "brpoplpush" we cannot modify the
     * queued item on transfer to the safety queue and we cannot update a timestamp to mark
     * the run start time in the message, so separate keys should be used for this.
     *
     * @param int $timeout
     * @return Message
     */
    public function waitAndReserve($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->checkClientConnection();
        $value = $this->client->brpoplpush("queue:{$this->name}:messages", "queue:{$this->name}:processing", $timeout);
        if (is_string($value)) {
            $message = $this->decodeMessage($value);
            if ($message->getIdentifier() !== null) {
                $this->client->sRem("queue:{$this->name}:ids", $message->getIdentifier());
            }
            return $message;
        } else {
            return null;
        }
    }

    /**
     * Mark a message as finished
     *
     * @param Message $message
     * @return boolean TRUE if the message could be removed
     */
    public function finish(Message $message)
    {
        $this->checkClientConnection();
        $originalValue = $message->getOriginalValue();
        $success = $this->client->lRem("queue:{$this->name}:processing", $originalValue, 0) > 0;
        if ($success) {
            $message->setState(Message::STATE_DONE);
        }
        return $success;
    }

    /**
     * Peek for messages
     *
     * @param integer $limit
     * @return Message[] Messages or empty array if no messages were present
     */
    public function peek($limit = 1)
    {
        $this->checkClientConnection();
        $result = $this->client->lRange("queue:{$this->name}:messages", -($limit), -1);
        if (is_array($result) && count($result) > 0) {
            $messages = array();
            foreach ($result as $value) {
                $message = $this->decodeMessage($value);
                // The message is still submitted and should not be processed!
                $message->setState(Message::STATE_SUBMITTED);
                $messages[] = $message;
            }
            return $messages;
        }
        return array();
    }

    /**
     * Count messages in the queue
     *
     * @return integer
     */
    public function count()
    {
        $this->checkClientConnection();
        $count = $this->client->lLen("queue:{$this->name}:messages");
        return $count;
    }

    /**
     * Encode a message
     *
     * Updates the original value property of the message to resemble the
     * encoded representation.
     *
     * @param Message $message
     * @return string
     */
    protected function encodeMessage(Message $message)
    {
        $value = json_encode($message->toArray());
        $message->setOriginalValue($value);
        return $value;
    }

    /**
     * Decode a message from a string representation
     *
     * @param string $value
     * @return Message
     */
    protected function decodeMessage($value)
    {
        $decodedMessage = json_decode($value, true);
        $message = new Message($decodedMessage['payload']);
        if (isset($decodedMessage['identifier'])) {
            $message->setIdentifier($decodedMessage['identifier']);
        }
        $message->setOriginalValue($value);
        return $message;
    }

    /**
     *
     * @param string $identifier
     * @return Message
     */
    public function getMessage($identifier)
    {
        return null;
    }

    /**
     * Check if the Redis client connection is still up and reconnect if Redis was disconnected
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
                throw new \Flowpack\JobQueue\Common\Exception('Could not connect to Redis', 1467382685);
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
        $database = isset($this->clientOptions['database']) ? $this->clientOptions['database'] : 0;

        // The connection read timeout should be higher than the timeout for blocking operations!
        $timeout = isset($this->clientOptions['timeout']) ? $this->clientOptions['timeout'] : round($this->defaultTimeout * 1.5);
        $connected = $this->client->connect($host, $port, $timeout) && $this->client->select($database);

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