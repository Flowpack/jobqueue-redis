<?php
namespace Jobqueue\Redis\Tests\Functional\Queue;

/*                                                                        *
 * This script belongs to the FLOW3 package "Jobqueue.Redis".                *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        *
 * The TYPO3 project - inspiring people to share!                         *
 *                                                                        */

/**
 * Functional test for RedisQueue
 */
class RedisQueueTest extends \TYPO3\FLOW3\Tests\FunctionalTestCase {

	/**
	 * @var \Jobqueue\Redis\Queue\RedisQueue
	 */
	protected $queue;

	/**
	 * Set up dependencies
	 */
	public function setUp() {
		parent::setUp();
		$configurationManager = $this->objectManager->get('TYPO3\FLOW3\Configuration\ConfigurationManager');
		$settings = $configurationManager->getConfiguration(\TYPO3\FLOW3\Configuration\ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'Jobqueue.Redis');
		if (isset($settings['testing']['enabled']) && $settings['testing']['enabled'] !== TRUE) {
			$this->markTestSkipped('Test database is not configured');
		}

		$this->queue = new \Jobqueue\Redis\Queue\RedisQueue('Test queue', $settings['testing']);

		$client = new \Predis\Client($settings['testing']);
		$client->flushdb();
	}

	/**
	 * @test
	 */
	public function publishAndWaitWithMessageWorks() {
		$message = new \Jobqueue\Common\Queue\Message('Yeah, tell someone it works!');
		$this->queue->publish($message);

		$result = $this->queue->waitAndTake(1);
		$this->assertNotNull($result, 'wait should receive message');
		$this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');
	}

	/**
	 * @test
	 */
	public function waitForMessageTimesOut() {
		$result = $this->queue->waitAndTake(1);
		$this->assertNull($result, 'wait should return NULL after timeout');
	}

	/**
	 * @test
	 */
	public function identifierMakesMessagesUnique() {
		$message = new \Jobqueue\Common\Queue\Message('Yeah, tell someone it works!', 'test.message');
		$identicalMessage = new \Jobqueue\Common\Queue\Message('Yeah, tell someone it works!', 'test.message');
		$this->queue->publish($message);
		$this->queue->publish($identicalMessage);

		$this->assertEquals(\Jobqueue\Common\Queue\Message::STATE_NEW, $identicalMessage->getState());

		$result = $this->queue->waitAndTake(1);
		$this->assertNotNull($result, 'wait should receive message');

		$result = $this->queue->waitAndTake(1);
		$this->assertNull($result, 'message should not be queued twice');
	}

	/**
	 * @test
	 */
	public function peekReturnsNextMessageIfQueueHasMessages() {
		$message = new \Jobqueue\Common\Queue\Message('First message');
		$this->queue->publish($message);
		$message = new \Jobqueue\Common\Queue\Message('Another message');
		$this->queue->publish($message);

		$result = $this->queue->peek();
		$this->assertNotNull($result, 'peek should return a message');
		$this->assertEquals('First message', $result->getPayload());
		$this->assertEquals(\Jobqueue\Common\Queue\Message::STATE_PUBLISHED, $result->getState());

		$result = $this->queue->peek();
		$this->assertEquals('First message', $result->getPayload(), 'second peek should return the same message again');
	}

	/**
	 * @test
	 */
	public function peekReturnsNullIfQueueHasNoMessage() {
		$result = $this->queue->peek();
		$this->assertNull($result, 'peek should not return a message');
	}

	/**
	 * @test
	 */
	public function waitAndReserveWithFinishRemovesMessage() {
		$message = new \Jobqueue\Common\Queue\Message('First message');
		$this->queue->publish($message);

		$result = $this->queue->waitAndReserve(1);
		$this->assertNotNull($result, 'waitAndReserve should receive message');
		$this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');

		$result = $this->queue->peek();
		$this->assertNull($result, 'no message should be present in queue');

		$finishResult = $this->queue->finish($message);
		$this->assertTrue($finishResult);
	}

}
?>