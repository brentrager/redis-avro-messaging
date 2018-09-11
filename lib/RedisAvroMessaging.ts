import RedisPubSub from './RedisPubSub';
import AvroSchemaCacheManager from './AvroSchemaCacheManager';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
import AvroNotificationProtocol from './AvroNotificationProtocol';
import { AvroNotificationProducer } from './AvroNotificationProducer';
import { Notification, Request } from './types';
import AvroNotificationConsumer from './AvroNotificationConsumer';

export default class RedisAvroMessaging {
    private redisPubSub: RedisPubSub;
    private avroSchemaCacheManager: AvroSchemaCacheManager;
    private avroSchemasProducedManager: AvroSchemasProducedManager;
    private notificationProtocol: AvroNotificationProtocol;

    constructor(redisHost: string, redisPort: number) {
        this.redisPubSub = new RedisPubSub(redisHost, redisPort);
        this.avroSchemaCacheManager = new AvroSchemaCacheManager(this.redisPubSub);
        this.avroSchemasProducedManager = new AvroSchemasProducedManager(this.redisPubSub);
        this.notificationProtocol = new AvroNotificationProtocol(this.avroSchemaCacheManager);
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

    async createAvroRequestClient(): Promise<void> {

    }

    async createAvroRequestHandler(): Promise<void> {

    }
}