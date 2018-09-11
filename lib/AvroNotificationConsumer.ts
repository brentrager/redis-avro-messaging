import { EventEmitter } from 'events';
import NotificationSchemasWithIds from './NotificationSchemasWithIds';
import RedisPubSub from './RedisPubSub';
import NotificationProtocol from './NotificationProtocol';
import { AVRO_NOTIFICATION_CHANNEL } from './constants';
import { ChannelMessage, Notification } from './types';

export default class AvroNotificationConsumer extends EventEmitter {
    private initialized = false;
    private notificationSchemasWithId: NotificationSchemasWithIds;

    constructor(private redisPubSub: RedisPubSub, private notificationProtocol: NotificationProtocol, private notificationSchema: Notification) {
        super();
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
                if (channelMessage.channel === AVRO_NOTIFICATION_CHANNEL) {
                    const nodeId = channelMessage.message.nodeId;
                    const notificationPayload = channelMessage.message.message as Notification;

                    const keyPayload = await this.notificationProtocol.parsePayload(notificationPayload.key, nodeId);
                    const valuePayload = await this.notificationProtocol.parsePayload(notificationPayload.value, nodeId);

                    const notification: Notification = {
                        topic: notificationPayload.topic,
                        key: keyPayload.deserialize(this.notificationSchema.key),
                        value: valuePayload.deserialize(this.notificationSchema.value)
                    };

                    this.emit('notification', notification.key, notification.value);
                }
            });

            await this.redisPubSub.subscribe(AVRO_NOTIFICATION_CHANNEL);

            this.initialized = true;
        }
    }
}