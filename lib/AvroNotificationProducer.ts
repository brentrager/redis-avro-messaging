import AvroNotificationSchemasWithIds from './AvroNotificationSchemasWithIds';
import RedisPubSub from './RedisPubSub';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
import AvroNotificationProtocol from './AvroNotificationProtocol';
import { Notification } from './types';
import { AVRO_NOTIFICATION_CHANNEL } from './constants';

export default class AvroNotificationProducer {
    private initialized = false;
    private notificationSchemasWithId: AvroNotificationSchemasWithIds;

    constructor(private redisPubSub: RedisPubSub, private avroSchemasProducedManager: AvroSchemasProducedManager,
                private notificationProtocol: AvroNotificationProtocol, private notificationSchema: Notification) {
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            this.notificationSchemasWithId = (await this.avroSchemasProducedManager.addNotificationSchema(this.notificationSchema))[0];
            this.initialized = true;
        }
    }

    async produce(notifcationKey: any, notificationVlaue: any): Promise<void> {
        const keyPayload = this.notificationProtocol.buildPayload(this.notificationSchemasWithId.key().id(),
            this.notificationSchemasWithId.key().schema(), notifcationKey);
        const valuePayload = this.notificationProtocol.buildPayload(this.notificationSchemasWithId.value().id(),
            this.notificationSchemasWithId.value().schema(), notificationVlaue);

        const notificationPayload: Notification = {
            topic: this.notificationSchemasWithId.schema().topic,
            key: keyPayload,
            value: valuePayload
        };

        await this.redisPubSub.publish(AVRO_NOTIFICATION_CHANNEL, notificationPayload);
    }
}