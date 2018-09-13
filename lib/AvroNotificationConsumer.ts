import { EventEmitter } from 'events';
import RedisPubSub from './RedisPubSub';
import AvroNotificationProtocol from './AvroNotificationProtocol';
import { AVRO_NOTIFICATION_CHANNEL } from './constants';
import { ChannelMessage, Notification, Message } from './types';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');

const log = new ContainerLogging('redis-avro-messaging', 'AvroNotificationConsumer');

export default class AvroNotificationConsumer extends EventEmitter {
    private initialized = false;
    private queueName: string;

    constructor(private redisPubSub: RedisPubSub, private notificationProtocol: AvroNotificationProtocol,
                private notificationSchema: Notification, private useQueue: boolean = true) {
        super();
        this.queueName = this.notificationSchema.topic;
    }

    private async processNotificationMessage(message: Message): Promise<Notification | undefined> {
        const nodeId = message.nodeId;
        const notificationPayload = message.message as Notification;
        let notification: Notification | undefined;

        if (this.notificationSchema.topic === notificationPayload.topic) {
            const keyPayload = await this.notificationProtocol.parsePayload(notificationPayload.key, nodeId);
            const valuePayload = await this.notificationProtocol.parsePayload(notificationPayload.value, nodeId);

            notification = {
                topic: notificationPayload.topic,
                key: keyPayload.deserialize(this.notificationSchema.key),
                value: valuePayload.deserialize(this.notificationSchema.value)
            };
        }

        return notification;
    }

    async initialize(): Promise<Array<Notification>> {
        const notifications: Array<Notification> = [];

        if (!this.initialized) {
            if (this.useQueue) {
                // First, if we are using the queue, get all the messages already in the queue and try to
                // read them.
                //
                // After this, we will read from the queue when we receive a published notification.
                const messages = await this.redisPubSub.catchUpQueueMessages(this.queueName);
                for (const message of messages) {
                    try {
                        const notification = await this.processNotificationMessage(message);
                        if (notification) {
                            notifications.push(notification);
                        }
                    } catch (error) {
                        log.error(`Error reading message from node ${message.nodeId}`);
                    }
                }
            }

            this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
                try {
                    if (channelMessage.channel === AVRO_NOTIFICATION_CHANNEL) {
                        const notificationPayload = channelMessage.message.message as Notification;

                        if (this.notificationSchema.topic === notificationPayload.topic) {
                            let notification: Notification | undefined;

                            // If we are using the queue, read the message from the queue.
                            if (this.useQueue) {
                                const message = await this.redisPubSub.popQueueMessage(this.queueName);
                                if (message) {
                                    notification = await this.processNotificationMessage(message);
                                }
                            }

                            // If we don't get the message from the queue, read it from the published notification.
                            if (!notification) {
                                notification = await this.processNotificationMessage(channelMessage.message);
                            }

                            if (notification) {
                                this.emit('notification', notification.key, notification.value);
                            }
                        }
                    }
                } catch (error) {
                    log.error(`Error getting message: ${error}`);
                }
            });

            await this.redisPubSub.subscribe(AVRO_NOTIFICATION_CHANNEL);
            await this.redisPubSub.createQueue(this.queueName);

            this.initialized = true;
        }

        return notifications;
    }
}