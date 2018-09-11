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

    async createAvroNotificationProducer(notificationSchema: Notification): Promise<AvroNotificationProducer> {
        const avroNotificationProducer = new AvroNotificationProducer(this.redisPubSub, this.avroSchemasProducedManager,
            this.notificationProtocol, notificationSchema);
        await avroNotificationProducer.initialize();

        return avroNotificationProducer;
    }

    async createAvroNotificationConsumer(notificationSchema: Notification): Promise<AvroNotificationConsumer> {
        const avroNotificationConsumer = new AvroNotificationConsumer(this.redisPubSub, this.notificationProtocol, notificationSchema);
        await avroNotificationConsumer.initialize();

        return avroNotificationConsumer;
    }

    async createAvroRequestClient(): Promise<AvroRequestClient> {
        const avroRequestClient = new AvroRequestClient(this.redisPubSub, this.avroSchemasProducedManager, this.requestProtocol);
        await avroRequestClient.initialize();

        return avroRequestClient;
    }

    async createAvroRequestHandler(serviceObj: ServiceObject, serviceDef: ServiceDefinition): Promise<AvroRequestHandler> {
        const avroRequestHandler = new AvroRequestHandler(this.redisPubSub, this.avroSchemasProducedManager, this.requestProtocol, serviceObj, serviceDef);
        await avroRequestHandler.initialize();

        return avroRequestHandler;
    }
}