import { EventEmitter } from 'events';
import * as Redis from 'ioredis';
import { RedisSMQPromise } from 'rsmq-promise';
import RedisSMQ from 'rsmq';
import { ChannelMessage, Message, Notification } from './types';
import { NODE_ID } from './constants';
import * as _ from 'lodash';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');

const log = new ContainerLogging('redis-avro-messaging', 'RedisPubSub');

export default class RedisPubSub extends EventEmitter {
    /*
     * This serves as the communication point for Redis Pub/Sub. It maintains two Redis connections
     * as publishing and subscribing cannot be done on one connection. It sends messags and incoming messages
     * are emitted as events.
     */
    private redisPub: Redis.Redis;
    private redisSub: Redis.Redis;
    rsmq: RedisSMQPromise;

    constructor(private redisHost: string, private redisPort: number) {
        super();

        try {
            this.redisPub = new Redis(redisPort, redisHost, { connectionName: `${NODE_ID}-PUB`});
            this.redisSub = new Redis(redisPort, redisHost, { autoResubscribe: true, connectionName: `${NODE_ID}-SUB` });
            this.rsmq = new RedisSMQPromise({ host: redisHost, port: redisPort, options: { connectionName: `${NODE_ID}-RSMQ`}});
        } catch (error) {
            log.error(`Unable to connect to Redis URL: '${redisHost}:${redisPort}'.`);
            throw error;
        }

        this.redisSub.on('message', (channel, message) => {
            const channelMessage: ChannelMessage = {
                channel,
                message: JSON.parse(message)
            };
            this.emit('message', channelMessage);
        });
    }

    async subscribe(...channels: Array<string>): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.redisSub.subscribe(channels, (err: any, count: any) => {
                if (err) {
                    log.error(`Error subscribing to channels: ${channels}. Error: ${err}`);

                    return reject(err);
                }
                log.verbose(`Subscribed to channels: ${channels}.`);

                return resolve();
            });
        });
    }

    unsubscribe(...channels: Array<string>): void {
        for (const channel of channels) {
            this.redisSub.unsubscribe(channel);
        }
    }

    async publish(channel: string, message: any): Promise<void> {
        try {
            const nodeMessage: Message = {
                nodeId: NODE_ID,
                message
            };
            await this.redisPub.publish(channel, JSON.stringify(nodeMessage));
        } catch (error) {
            log.error(`Error publishing on channel ${channel}`);
            log.verbose(`Error publishing on channel ${channel}: ${message}`);
        }
    }

    async createQueue(queueName: string): Promise<void> {
        try {
            const queues = await this.rsmq.listQueues();
            const findQueue = queues.find(x => x === queueName);
            if (!findQueue) {
                await this.rsmq.createQueue({ qname: queueName, maxsize: -1 });
            }
        } catch (error) {
            log.warn(`Error creating RSMQ queue (this could be benign): ${error}`);
        }
    }

    async sendQueueMessage(queueName: string, message: any): Promise<void> {
        try {
            const nodeMessage: Message = {
                nodeId: NODE_ID,
                message
            };
            const id = await this.rsmq.sendMessage({ qname: queueName, message: JSON.stringify(nodeMessage) });
            log.verbose(`Sent message ${id} on queue ${queueName}`);
        } catch (error) {
            log.error(`Error sending RSMQ message on ${queueName}: ${error}`);
            throw error;
        }
    }

    async popQueueMessage(queueName: string): Promise<Message | undefined> {
        try {

            const messageInfo = await this.rsmq.popMessage({ qname: queueName }) as RedisSMQ.QueueMessage;
            let message: Message | undefined;

            if (!_.isEmpty(messageInfo)) {
                message = JSON.parse(messageInfo.message);
                log.verbose(`Received message ${messageInfo.id} on queue  ${queueName} originally sent at ${messageInfo.sent}`);
            }

            return message;
        } catch (error) {
            log.error(`Error sending RSMQ message on ${queueName}: ${error}`);
            throw error;
        }
    }

    async catchUpQueueMessages(queueName: string): Promise<Array<Message>> {
        const messages: Array<Message> = [];

        while (true) {
            const message = await this.popQueueMessage(queueName);
            if (message) {
                messages.push(message);
            } else {
                break;
            }
        }

        return messages;
    }
}