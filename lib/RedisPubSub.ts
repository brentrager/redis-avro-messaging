import EventEmitter from 'events';
import * as Redis from 'ioredis';
import { ChannelMessage, Message } from './types';
import { NODE_ID } from './constants';
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

    constructor(redisHost: string, redisPort: number) {
        super();

        try {
            this.redisPub = new Redis(redisPort, redisHost);
            this.redisSub = new Redis(redisPort, redisHost, { autoResubscribe: true });
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
}