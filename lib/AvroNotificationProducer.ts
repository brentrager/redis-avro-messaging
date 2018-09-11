import NotificationSchemasWithIds from './NotificationSchemasWithIds';
import RedisPubSub from './RedisPubSub';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
import NotificationProtocol from './NotificationProtocol';
import { Notification } from './types';
import { AVRO_NOTIFICATION_CHANNEL } from './constants';

export class AvroNotificationProducer {
    private initialized = false;
    private notificationSchemasWithId: NotificationSchemasWithIds;

    constructor(private redisPubSub: RedisPubSub, private avroSchemasProducedManager: AvroSchemasProducedManager,
                private notificationProtocol: NotificationProtocol, private notificationSchema: Notification) {
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            this.notificationSchemasWithId = (await this.avroSchemasProducedManager.addNotificationSchema(this.notificationSchema))[0];
            this.initialized = true;
        }
    }

    async produce(notifcation: Notification): Promise<void> {
        const keyPayload = this.notificationProtocol.buildPayload(this.notificationSchemasWithId.key().id(),
            this.notificationSchemasWithId.key().schema(), notifcation.key);
        const valuePayload = this.notificationProtocol.buildPayload(this.notificationSchemasWithId.value().id(),
            this.notificationSchemasWithId.value().schema(), notifcation.value);

        const notificationPayload: Notification = {
            topic: this.notificationSchemasWithId.schema().topic,
            key: keyPayload,
            value: valuePayload
        };

        await this.redisPubSub.publish(AVRO_NOTIFICATION_CHANNEL, notificationPayload);
    }
}