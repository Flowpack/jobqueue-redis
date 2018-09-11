<?php
namespace Flowpack\JobQueue\Redis\Tests\Functional\Queue;

/*
 * This file is part of the Flowpack.JobQueue.Redis package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Flowpack\JobQueue\Redis\Queue\RedisQueue;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\Tests\FunctionalTestCase;
use Neos\Utility\TypeHandling;

/**
 * Functional test for RedisAuthenticationTest
 */
class RedisAuthenticationTest extends FunctionalTestCase
{

    /**
     * @var QueueInterface
     */
    protected $queue;

    /**
     * @var array
     */
    protected $queueSettings;

    /**
     * @var array
     */
    protected $options = [];

    /**
     * Set up dependencies
     */
    public function setUp()
    {
        parent::setUp();
        $configurationManager = $this->objectManager->get(ConfigurationManager::class);
        $packageKey = $this->objectManager->getPackageKeyByObjectName(
            TypeHandling::getTypeForValue($this)
        );
        $packageSettings = $configurationManager->getConfiguration(
            ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, $packageKey
        );
        if (!isset($packageSettings['testing']['enabled']) || $packageSettings['testing']['enabled'] !== true) {
            $this->markTestSkipped(sprintf('Queue is not configured (%s.testing.enabled != TRUE)', $packageKey));
        }
        $this->queueSettings = $packageSettings['testing'];
        $this->options['client'] = $this->queueSettings['passwordProtectedClient'];
        $this->queue = new RedisQueue('Test queue', $this->options);
        $this->queue->flush();
    }

    /**
     * In this testcase we use the password 'Horst_Schimanski'
     * @test
     */
    public function testConnectionByAddingAMessage()
    {
        $this->queue->submit('First message');
        $this->assertSame(1, $this->queue->countReady());
    }
}