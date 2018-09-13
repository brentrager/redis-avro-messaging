import RedisPubSub from './RedisPubSub';
import AvroSchemaCacheManager from './AvroSchemaCacheManager';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
import AvroNotificationProtocol from './AvroNotificationProtocol';
import AvroNotificationProducer from './AvroNotificationProducer';
import { Notification, Request } from './types';
import AvroNotificationConsumer from './AvroNotificationConsumer';
import AvroRequestClient from './AvroRequestClient';
import AvroRequestProtocol from './AvroRequestProtocol';
import { AvroRequestHandler, ServiceDefinition, ServiceObject } from './AvroRequestHandler';

export default class RedisAvroMessaging {
    private redisPubSub: RedisPubSub;
    private avroSchemaCacheManager: AvroSchemaCacheManager;
    private avroSchemasProducedManager: AvroSchemasProducedManager;
    private notificationProtocol: AvroNotificationProtocol;
    private requestProtocol: AvroRequestProtocol;

    constructor(redisHost: string, redisPort: number) {
        this.redisPubSub = new RedisPubSub(redisHost, redisPort);
        this.avroSchemaCacheManager = new AvroSchemaCacheManager(this.redisPubSub);
        this.avroSchemasProducedManager = new AvroSchemasProducedManager(this.redisPubSub);
        this.notificationProtocol = new AvroNotificationProtocol(this.avroSchemaCacheManager);
        this.requestProtocol = new AvroRequestProtocol(this.avroSchemaCacheManager);
    }

    /**
     * Create a producer for an Avro Notification.
     *
     * This queues up messages in Redis which can be consumed in order at will.
     *
     * @param notificationSchema Schema of the notification to be sent
     * @param useQueue Whether to append to the notification queue.
     */
    async createAvroNotificationProducer(notificationSchema: Notification, useQueue = true): Promise<AvroNotificationProducer> {
        const avroNotificationProducer = new AvroNotificationProducer(this.redisPubSub, this.avroSchemasProducedManager,
            this.notificationProtocol, notificationSchema, useQueue);
        await avroNotificationProducer.initialize();

        return avroNotificationProducer;
    }

    /**
     * Create a consumer for an Avro Notification.
     *
     * Ideally, only one consumer exists for a notification. There is a queue in redis that is consumed by this
     * to allow disaster recovery. If there are multiple consumers, they will not both be able to read the queue.
     *
     * @param notificationSchema Schema of the notification to be consumed.
     * @param useQueue Whether to consume from the notification queue.
     */
    async createAvroNotificationConsumer(notificationSchema: Notification, useQueue = true): Promise<AvroNotificationConsumer> {
        const avroNotificationConsumer = new AvroNotificationConsumer(this.redisPubSub, this.notificationProtocol, notificationSchema, useQueue);
        await avroNotificationConsumer.initialize();

        return avroNotificationConsumer;
    }

    /**
     * Create a client to send Avro Reqeusts and receive responses.
     *
     * Requests can fail and timeout. As such, they are only published and subscribed. There is no queueing
     * or other considerations of disaster recovery here.
     */
    async createAvroRequestClient(): Promise<AvroRequestClient> {
        const avroRequestClient = new AvroRequestClient(this.redisPubSub, this.avroSchemasProducedManager, this.requestProtocol);
        await avroRequestClient.initialize();

        return avroRequestClient;
    }

    /**
     * Create a handler to handle Avro Requests and send responses.
     *
     * Requests can fail and timeout. As such, they are only published and subscribed. There is no queueing
     * or other considerations of disaster recovery here.
     *
     * @param serviceObj Object which has methods that correspond to the request names that handle the requests and return responses.
     * @param serviceDef The request schemas that are handled.
     */
    async createAvroRequestHandler(serviceObj: ServiceObject, serviceDef: ServiceDefinition): Promise<AvroRequestHandler> {
        const avroRequestHandler = new AvroRequestHandler(this.redisPubSub, this.avroSchemasProducedManager, this.requestProtocol, serviceObj, serviceDef);
        await avroRequestHandler.initialize();

        return avroRequestHandler;
    }
}