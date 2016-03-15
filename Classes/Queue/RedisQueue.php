<?php
namespace Flowpack\JobQueue\Redis\Queue;

use TYPO3\Flow\Package\Package as BasePackage;
use TYPO3\Flow\Annotations as Flow;

/**
 * A queue implementation using Redis as the queue backend
 *
 * Depends on Predis as the PHP Redis client.
 */
class RedisQueue implements \Flowpack\JobQueue\Common\Queue\QueueInterface {

	/**
	 * @var string
	 */
	protected $name;

	/**
	 * @var \Predis\Client
	 */
	protected $client;

	/**
	 * @var integer
	 */
	protected $defaultTimeout = 60;

	/**
	 * Constructor
	 *
	 * @param string $name
	 * @param array $options
	 */
	public function __construct($name, array $options = array()) {
		$this->name = $name;
		if (isset($options['defaultTimeout'])) {
			$this->defaultTimeout = (integer)$options['defaultTimeout'];
		}
		$clientOptions = isset($options['client']) ? $options['client'] : array();
		$this->client = new \Predis\Client($clientOptions);
	}

	/**
	 * Publish a message to the queue
	 *
	 * @param \Flowpack\JobQueue\Common\Queue\Message $message
	 * @return void
	 */
	public function publish(\Flowpack\JobQueue\Common\Queue\Message $message) {
		if ($message->getIdentifier() !== NULL) {
			$added = $this->client->sadd("queue:{$this->name}:ids", $message->getIdentifier());
			if (!$added) {
				return;
			}
		}
		$encodedMessage = $this->encodeMessage($message);
		$this->client->lpush("queue:{$this->name}:messages", $encodedMessage);
		$message->setState(\Flowpack\JobQueue\Common\Queue\Message::STATE_PUBLISHED);
	}

	/**
	 * Wait for a message in the queue and return the message for processing
	 * (without safety queue)
	 *
	 * @param int $timeout
	 * @return \Flowpack\JobQueue\Common\Message The received message or NULL if a timeout occured
	 */
	public function waitAndTake($timeout = NULL) {
		$timeout !== NULL ? $timeout : $this->defaultTimeout;
		$keyAndValue = $this->client->brpop("queue:{$this->name}:messages", $timeout);
		$value = $keyAndValue[1];
		if (is_string($value)) {
			$message = $this->decodeMessage($value);

			if ($message->getIdentifier() !== NULL) {
				$this->client->srem("queue:{$this->name}:ids", $message->getIdentifier());
			}

				// The message is marked as done
			$message->setState(\Flowpack\JobQueue\Common\Queue\Message::STATE_DONE);

			return $message;
		} else {
			return NULL;
		}
	}

	/**
	 * Wait for a message in the queue and save the message to a safety queue
	 *
	 * TODO: Idea for implementing a TTR (time to run) with monitoring of safety queue. E.g.
	 * use different queue names with encoded times? With brpoplpush we cannot modify the
	 * queued item on transfer to the safety queue and we cannot update a timestamp to mark
	 * the run start time in the message, so separate keys should be used for this.
	 *
	 * @param int $timeout
	 * @return \Flowpack\JobQueue\Common\Queue\Message
	 */
	public function waitAndReserve($timeout = NULL) {
		$timeout !== NULL ? $timeout : $this->defaultTimeout;
		$value = $this->client->brpoplpush("queue:{$this->name}:messages", "queue:{$this->name}:processing", $timeout);
		if (is_string($value)) {
			$message = $this->decodeMessage($value);
			if ($message->getIdentifier() !== NULL) {
				$this->client->srem("queue:{$this->name}:ids", $message->getIdentifier());
			}
			return $message;
		} else {
			return NULL;
		}
	}

	/**
	 * Mark a message as finished
	 *
	 * @param \Flowpack\JobQueue\Common\Queue\Message $message
	 * @return boolean TRUE if the message could be removed
	 */
	public function finish(\Flowpack\JobQueue\Common\Queue\Message $message) {
		$originalValue = $message->getOriginalValue();
		$success = $this->client->lrem("queue:{$this->name}:processing", 0, $originalValue) > 0;
		if ($success) {
			$message->setState(\Flowpack\JobQueue\Common\Queue\Message::STATE_DONE);
		}
		return $success;
	}

	/**
	 * Peek for messages
	 *
	 * @param integer $limit
	 * @return array Messages or empty array if no messages were present
	 */
	public function peek($limit = 1) {
		$result = $this->client->lrange("queue:{$this->name}:messages", -($limit), -1);
		if (is_array($result) && count($result) > 0) {
			$messages = array();
			foreach ($result as $value) {
				$message = $this->decodeMessage($value);
					// The message is still published and should not be processed!
				$message->setState(\Flowpack\JobQueue\Common\Queue\Message::STATE_PUBLISHED);
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
	public function count() {
		$count = $this->client->llen("queue:{$this->name}:messages");
		return $count;
	}

	/**
	 * Encode a message
	 *
	 * Updates the original value property of the message to resemble the
	 * encoded representation.
	 *
	 * @param \Flowpack\JobQueue\Common\Queue\Message $message
	 * @return string
	 */
	protected function encodeMessage(\Flowpack\JobQueue\Common\Queue\Message $message) {
		$value = json_encode($message->toArray());
		$message->setOriginalValue($value);
		return $value;
	}

	/**
	 * Decode a message from a string representation
	 *
	 * @param string $value
	 * @return \Flowpack\JobQueue\Common\Queue\Message
	 */
	protected function decodeMessage($value) {
		$decodedMessage = json_decode($value, TRUE);
		$message = new \Flowpack\JobQueue\Common\Queue\Message($decodedMessage['payload']);
		if (isset($decodedMessage['identifier'])) {
			$message->setIdentifier($decodedMessage['identifier']);
		}
		$message->setOriginalValue($value);
		return $message;
	}

	/**
	 *
	 * @param string $identifier
	 * @return \Flowpack\JobQueue\Common\Queue\Message
	 */
	public function getMessage($identifier) {
		return NULL;
	}


}
?>