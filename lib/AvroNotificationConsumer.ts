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
    private paused = false;
    private queuedNotifications: Array<Notification> = [];

    isPaused(): boolean {
        return this.paused;
    }

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

    private async getCatchUpNotifications(): Promise<Array<Notification>> {
        const notifications: Array<Notification> = [];
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

        return notifications;
    }

    private sendCatchUpNotifications(notifications: Array<Notification>): void {
        for (const notification of notifications) {
            this.emit('notification', notification.key, notification.value);
        }
    }

    async initialize(): Promise<void> {

        if (!this.initialized) {
            if (this.useQueue) {
                await this.redisPubSub.createQueue(this.queueName);

                // First, if we are using the queue, get all the messages already in the queue and try to
                // read them.
                //
                // After this, we will read from the queue when we receive a published notification.
                const notifications = await this.getCatchUpNotifications();

                if (notifications.length) {
                    const catchUpPromise = new Promise<void>((resolve) => {
                        const interval = setInterval(() => {
                            if (this.listenerCount('notification')) {
                                this.sendCatchUpNotifications(notifications);
                                clearInterval(interval);

                                return resolve();
                            }
                        }, 100);
                    });

                    catchUpPromise.catch(error => {
                        log.error(`Error catching up with queue messages for notification ${this.queueName}: ${error}`);
                    });
                }
            }

            this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
                try {
                    if (channelMessage.channel === AVRO_NOTIFICATION_CHANNEL) {
                        const notificationPayload = channelMessage.message.message as Notification;

                        if (this.notificationSchema.topic === notificationPayload.topic) {
                            let notification: Notification | undefined;

                            // If we are using the queue, read the message from the queue.
                            if (!this.paused && this.useQueue) {
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
                                if (!this.paused) {
                                    this.emit('notification', notification.key, notification.value);
                                } else {
                                    if (!this.useQueue) {
                                        this.queuedNotifications.push(notification);
                                    }
                                }
                            }
                        }
                    }
                } catch (error) {
                    log.error(`Error getting message: ${error}`);
                }
            });

            await this.redisPubSub.subscribe(AVRO_NOTIFICATION_CHANNEL);

            this.initialized = true;
        }
    }

    /**
     * When paused, any notifications will be queued.
     */
    pause(): void {
        this.paused = true;
    }

    /**
     * When resumed, if any notifications were queued while paused, they will be emitted.
     */
    resume(): void {
            const resumePromise = new Promise(async (resolve) => {
                if (this.paused) {
                    if (this.useQueue) {
                        const notifications = await this.getCatchUpNotifications();
                        this.sendCatchUpNotifications(notifications);
                    } else {
                        this.sendCatchUpNotifications(this.queuedNotifications);
                        this.queuedNotifications = [];
                    }
                }

                this.paused = false;

                return resolve();
            });

            resumePromise.catch((error) => {
                log.error(`Error resuming for notification ${this.queueName}`);
            });
    }
}