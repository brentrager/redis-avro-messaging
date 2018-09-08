import * as avsc from 'avsc';
import Redis from 'ioredis';
import * as url from 'url';
import * as uuid from 'uuid';
import * as EventEmitter from 'events';
import * as _ from 'lodash';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');
// tslint:disable-next-line:variable-name no-require-imports
const icAvroLib = require('@pureconnect/icavrolib');

const log = new ContainerLogging('redis-avro-messaging', 'RedisAvroMessaging');

const _nodeId = uuid.v4();
const AVRO_SCHEMA_REGISTER_CHANNEL = 'avroSchemaRegisterChannel';
const AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL = 'avroSchemaRegisterRequesthannel';
const REQUEST_CHANNEL = 'requestChannel';
const RESPONSE_CHANNEL = 'responseChannel';
const NOTIFICATION_CHANNEL = 'notificationChannel';

interface Message {
    nodeId: string;
    message: any;
}

class ExpiringPromise {
    /*
     * Meant to be used as ExpiringPromise.waitWithTimeout(promise, timeoutMs).
     *
     * Constructor is not meant to be called directly.
     */
    private timeoutPromise: Promise<void>;

    constructor(private promise: Promise<any>, private timeoutMs: number = 10000) {
        this.timeoutPromise = new Promise((_resolve, reject) => {
            setTimeout(() => {
                const error = new Error('Promise timed out.');

                return reject(error);
            }, timeoutMs);
        });
    }

    async wait(): Promise<any> {
        return Promise.race([this.promise, this.timeoutPromise]);
    }

    static async waitWithTimeout(promise: Promise<any>, timeoutMs: number = 10000): Promise<any> {
        const expiringPromise = new ExpiringPromise(promise, timeoutMs);

        return expiringPromise.wait();
    }
}

class RedisPubSub extends EventEmitter {
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
            this.emit('message', { channel, message: JSON.parse(message) });
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
                nodeId: _nodeId,
                message
            };
            await this.redisPub.publish(channel, JSON.stringify(nodeMessage));
        } catch (error) {
            log.error(`Error publishing on channel ${channel}`);
            log.verbose(`Error publishing on channel ${channel}: ${message}`);
        }
    }
}

class AvroSchemaCache {
    /*
     * This cache is a cache of avro schemas from other microservices running this package. This is a map
     * of _nodeId for this package to a map of schema IDs to schemas.
     *
     * Each service will keep an array of schemas it writes. The schema ID is the position of that schema
     * in the array.
     */
    private cache: Map<string, Map<number, any>> = new Map();

    constructor() {
    }

    setSchemas(nodeId: string, schemas: Array<any>): void {
        this.cache.set(nodeId, schemas.reduce((result, schema, index) => {
            result.set(index, schema);
        }, new Map()));
    }

    getSchema(nodeId: string, schemaId: number): any {
        let result: any;
        const schemaMap = this.cache.get(nodeId);
        if (schemaMap) {
            result = schemaMap.get(schemaId);
        }

        return result;
    }
}

class AvroSchemaCacheManager extends EventEmitter {
    private schemaCache: AvroSchemaCache;
    /*
     * This is a wrapper around AvroSchemaCache which proactively requests missing schemas from other microservices.
     * If this microservice started after another, it may be missing some of the other microservices schemas. If we
     * need a schema and don't have it, this class will request it.
     */
    constructor(private redisPubSub: RedisPubSub) {
        super();
        this.schemaCache = new AvroSchemaCache();

        this.redisPubSub.on('message', channelMessage => {
            if (channelMessage.channel === AVRO_SCHEMA_REGISTER_CHANNEL) {
                const message = channelMessage.message;
                const nodeId = message.nodeId;
                const schemas = message.message as Array<any>;
                this.schemaCache.setSchemas(nodeId, schemas);
                this.emit('schemasRegistered', { nodeId, schemas });
            }
        });

        this.redisPubSub.subscribe(AVRO_SCHEMA_REGISTER_CHANNEL).then(() => {
            log.verbose('Listening for schema registrations.');
        });
    }

    setSchemas(nodeId: string, schemas: Array<any>): void {
        this.schemaCache.setSchemas(nodeId, schemas);
    }

    async getSchema(nodeId: string, schemaId: number): Promise<any> {
        let result = this.schemaCache.getSchema(nodeId, schemaId);

        if (!result) {
            try {
                result = await ExpiringPromise.waitWithTimeout(new Promise(async (resolve, reject) => {
                    this.on('schemasRegistered', schemasRegistered => {
                        if (schemasRegistered.nodeId === nodeId) {
                            const innerResult = this.schemaCache.getSchema(nodeId, schemaId);

                            if (!result) {
                                const error = new Error(`Schema ${schemaId} does not exist for node ${nodeId}.`);
                                log.error(error);

                                return reject(error);
                            }

                            return resolve(innerResult);
                        }
                    });

                    await this.redisPubSub.publish(AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL, nodeId);
                }));
            } catch (error) {
                log.error(`Error getting schema ${schemaId} for node ${nodeId}.`);
            }
        }

        return result;
    }
}

class AvroSchemasProducedManager {
    /*
     * This class keeps track of all the schemas for which this microservice produces messages. This could be
     * either produced notifications or requests made to other microservies and the expected responses.
     */
    private schemasProduced: Array<any>;

    private async notifySchemasProduced(): Promise<void> {
        return this.redisPubSub.publish(AVRO_SCHEMA_REGISTER_CHANNEL, this.schemasProduced);
    }

    constructor(private redisPubSub: RedisPubSub) {
        this.schemasProduced = [];

        this.redisPubSub.on('message', async channelMessage => {
            if (channelMessage.channel === AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL) {
                const nodeId = channelMessage.message;
                // If it's me, send my schemas out.
                if (nodeId === _nodeId) {
                    await this.notifySchemasProduced();
                }
            }
        });

        this.redisPubSub.subscribe(AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL).then(() => {
            log.verbose('Listening for schema register requests.');
        });
    }
    async addProducedSchemas(...schemas: Array<any>): Promise<Array<number>> {
        this.schemasProduced.concat(schemas);

        await this.notifySchemasProduced();

        // Return an array of schema IDs that represent the newly added schemas
        return _.range(this.schemasProduced.length - schemas.length, this.schemasProduced.length - 1);
    }
}

export class RedisAvroMessaging {
    private redisPubSub: RedisPubSub;
    private avroSchemaCacheManager: AvroSchemaCacheManager;

    constructor(redisHost: string, redisPort: number) {
        this.redisPubSub = new RedisPubSub(redisHost, redisPort);
        this.avroSchemaCacheManager = new AvroSchemaCacheManager(this.redisPubSub);
    }

    async createAvroProducer(): Promise<void> {

    }

    async createAvroConsumer(): Promise<void> {

    }

    async reateAvroRequestClient(): Promise<void> {

    }

    async createAvroRequestHandler(): Promise<void> {

    }
}
