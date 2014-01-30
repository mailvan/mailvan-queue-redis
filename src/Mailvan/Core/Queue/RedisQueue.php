<?php


namespace Mailvan\Core\Queue;


use Mailvan\Core\Request\RequestInterface;
use Mailvan\Core\Request\RequestFactoryInterface;
use Predis\ClientInterface;

/**
 * Class RedisQueue
 * This class implements process of retrieving mailvan commands from Redis storage
 *
 * @package Mailvan\Core\Queue
 * @author Alex Panshin <deadyaga@gmail.com>
 */
class RedisQueue implements QueueInterface
{
    /**
     * @var RequestFactoryInterface
     */
    private $factory;

    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * Stores the key in which queue is stored
     * @var string
     */
    private $key;


    public function __construct(ClientInterface $client, $key, RequestFactoryInterface $factory = null)
    {
        if (!is_null($factory)) {
            $this->setRequestFactory($factory);
        }

        $this->key = $key;
        $this->client = $client;
    }

    /**
     * This method is a way to inject your own RequestFactory instance into
     *
     * @param RequestFactoryInterface $factory
     * @return void
     */
    public function setRequestFactory(RequestFactoryInterface $factory)
    {
        $this->factory = $factory;
    }

    /**
     * Pops first element from queue, creates Request instance from it and returns it.
     * Method must try to unserialize popped element before creating Request instance
     *
     * @return RequestInterface
     */
    public function pop()
    {
        $data = $this->client->lpop($this->key);

        return is_null($data)
            ? null
            : $this->factory->createRequest(unserialize($data));
    }

    /**
     * Pushes given $data as a serialized string
     *
     * @param $data
     * @return void
     */
    public function push($data)
    {
        $this->client->rpush($this->key, is_string($data) ? $data : serialize($data));
    }

    /**
     * Returns current queue length
     *
     * @return int
     */
    public function getLength()
    {
        return $this->client->llen($this->key);
    }
}