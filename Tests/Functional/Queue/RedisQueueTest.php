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

use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;
use Flowpack\JobQueue\Redis\Queue\RedisQueue;

/**
 * Functional test for RedisQueue
 */
class RedisQueueTest extends AbstractQueueTest
{
    /**
     * @inheritdoc
     */
    protected function getQueue()
    {
        return new RedisQueue('Test queue', $this->queueSettings);
    }
}