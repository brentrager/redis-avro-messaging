import Redis from 'ioredis';
import * as uuid from 'uuid';
import * as EventEmitter from 'events';
import _ from 'lodash';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');
// tslint:disable-next-line:variable-name no-require-imports
const icAvroLib = require('@pureconnect/icavrolib');

const log = new ContainerLogging('redis-avro-messaging', 'RedisAvroMessaging');

const _nodeId = uuid.v4();
const AVRO_SCHEMA_REGISTER_CHANNEL = 'avroSchemaRegisterChannel';
const AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL = 'avroSchemaRegisterRequesthannel';
const AVRO_REQUEST_CHANNEL = 'avroRequestChannel';
const AVRO_RESPONSE_CHANNEL = 'avroResponseChannel';
const AVRO_NOTIFICATION_CHANNEL = 'avroNotificationChannel';

interface Message {
    nodeId: string;
    message: any;
}

interface ChannelMessage {
    channel: string;
    message: Message;
}

export interface Notification {
    topic: string;
    key: any;
    value: any;
}

export interface Request {
    request: any;
    response: any;
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

class AvroSchemaWithId {
    constructor(private _id: number, private _schema: any) {
    }

    schema(): any {
        return this._schema;
    }

    id(): number {
        return this._id;
    }
}

class NotificationSchemasWithIds {
    constructor(private _schema: Notification, private _key: AvroSchemaWithId, private _value: AvroSchemaWithId) {
    }

    schema(): Notification {
        return this._schema;
    }

    key(): AvroSchemaWithId {
        return this._key;
    }

    value(): AvroSchemaWithId {
        return this._value;
    }
}

class RequestSchemasWithIds {
    constructor(private _schema: Request, private _request: AvroSchemaWithId, private _response: AvroSchemaWithId) {
    }

    schema(): Request {
        return this._schema;
    }

    request(): AvroSchemaWithId {
        return this._request;
    }

    response(): AvroSchemaWithId {
        return this._response;
    }
}

class NotificationProtocol {
    private protocolVersion = 0;
    private offsets = {
        protocolVersion: 0,
        schemaId: 1,
        data: 5
    };

    constructor(private avroSchemaCacheManager: AvroSchemaCacheManager) {
    }

    /**
     * Build a payload (either a key or value for a notification).  Returns a Buffer containing the
     * payload.
     */
    buildPayload(schemaId: number, avroType: any, data: any): string {
        const dataLen = icAvroLib.getAvroLength(avroType, data);
        const payload = Buffer.allocUnsafe(this.offsets.data + dataLen);

        payload.writeUInt8(this.protocolVersion, this.offsets.protocolVersion);
        payload.writeUInt32BE(schemaId, this.offsets.schemaId);
        icAvroLib.avroSerialize(avroType, payload, this.offsets.data, dataLen, data);

        return payload.toString('base64');
    }

    /**
     * Parse a payload (either a key or value from a notification).  Retrieves the schema using
     * schemaCache and parses the payload.
     *
     * If the protocol version is not supported, the promise is rejected.
     *
     * @param payload - A Buffer holding the key or value payload from the notification
     * @param schemaCache - A SchemaRegistryCache used to retrieve the sender's schema
     * @return Promise - an AvroReceivePayload that can be used to deserialize the data
     */
    async parsePayload(payloadString: string, nodeId: string): Promise<any> {
        const payload = Buffer.from(payloadString, 'base64');

        if (!payload) {
            throw new Error('Cannot parse null payload');
        }
        if (payload.length < this.offsets.data) {
            throw new Error(`Payload size ${payload.length} is invalid`);
        }

        // Read the protocol version
        const msgProtocolVersion = payload.readUInt8(this.offsets.protocolVersion);
        // If the protocol version is not supported, reject the promise
        if (msgProtocolVersion !== this.protocolVersion) {
            throw new Error(`Payload uses unsupported protocol version ${msgProtocolVersion}`);
        }

        // Read the schema ID
        const schemaId = payload.readUInt32BE(this.offsets.schemaId);
        // Slice off the data section
        const dataBuf = payload.slice(this.offsets.data);

        const schema = await this.avroSchemaCacheManager.getSchema(nodeId, schemaId);

        const dataType = icAvroLib.getParsedType(schema);

        return new icAvroLib.AvroReceivePayload(dataBuf, dataType);
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
    private cache: Map<string, Map<number, AvroSchemaWithId>> = new Map();

    constructor() {
    }

    setSchemas(nodeId: string, schemas: Array<any>): void {
        this.cache.set(nodeId, schemas.reduce((result, schema, index) => {
            result.set(index, new AvroSchemaWithId(index, schema));
        }, new Map()));
    }

    getSchema(nodeId: string, schemaId: number): AvroSchemaWithId {
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

        this.redisPubSub.on('message', (channelMessage: ChannelMessage) => {
            if (channelMessage.channel === AVRO_SCHEMA_REGISTER_CHANNEL) {
                const message = channelMessage.message;
                const nodeId = message.nodeId;
                const schemas = message.message.schemas as Array<any>;
                this.schemaCache.setSchemas(nodeId, schemas);
                this.emit('schemasRegistered', { nodeId, schemas });
            }
        });

        this.redisPubSub.subscribe(AVRO_SCHEMA_REGISTER_CHANNEL).then(() => {
            log.verbose('Listening for schema registrations.');
        }).catch(error => {
            log.error(`Error listening for schema registrations: ${error}`);
            throw error;
        });
    }

    setSchemas(nodeId: string, schemas: Array<any>): void {
        this.schemaCache.setSchemas(nodeId, schemas);
    }

    async getSchema(nodeId: string, schemaId: number): Promise<AvroSchemaWithId> {
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

                    await this.redisPubSub.publish(AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL, { nodeId });
                }));
            } catch (error) {
                log.error(`Error getting schema ${schemaId} for node ${nodeId}.`);
                throw error;
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
    private schemasProduced: Array<AvroSchemaWithId>;

    private async notifySchemasProduced(): Promise<Array<any>> {
        const schemas = this.schemasProduced.map(x => x.schema());
        await this.redisPubSub.publish(AVRO_SCHEMA_REGISTER_CHANNEL, { schemas });

        log.verbose(`Notified other services of ${schemas.length} schemas produced.`);

        return schemas;
    }

    constructor(private redisPubSub: RedisPubSub) {
        this.schemasProduced = [];

        this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
            if (channelMessage.channel === AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL) {
                const nodeId = channelMessage.message.message.nodeId;
                // If it's me, send my schemas out.
                if (nodeId === _nodeId) {
                    await this.notifySchemasProduced();
                }
            }
        });

        this.redisPubSub.subscribe(AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL).then(() => {
            log.verbose('Listening for schema register requests.');
        }).catch(error => {
            log.error(`Error listening for schema register requests: ${error}`);
            throw error;
        });
    }

    private async addProducedSchema(...schemas: Array<any>): Promise<Array<AvroSchemaWithId>> {
        const existingSchemasWithId = [];
        const schemasWithId = [];
        let index = this.schemasProduced.length;
        for (const schema of schemas) {
            const schemaWithId = this.schemasProduced.find(_schemaWithId => {
                return _.isEqual(_schemaWithId.schema, schema);
            });
            if (schemaWithId) {
                existingSchemasWithId.push(schemaWithId);
            } else {
                schemasWithId.push(new AvroSchemaWithId(index, schema));
                index++;
            }
        }
        this.schemasProduced.concat(schemas);

        await this.notifySchemasProduced();

        // Return an array of schema IDs that represent the newly added schemas
        return existingSchemasWithId.concat(schemasWithId);
    }

    async addNotificationSchema(...schemas: Array<Notification>): Promise<Array<NotificationSchemasWithIds>> {
        const notificationSchemasWithIds = [] as Array<NotificationSchemasWithIds>;
        const schemaTypes = [] as Array<any>;
        for (const notification of schemas) {
            schemaTypes.push(notification.key);
            schemaTypes.push(notification.value);
        }

        const schemasWithIds = await this.addProducedSchema(schemaTypes);

        let index = 0;
        for (const notification of schemas) {
            notificationSchemasWithIds.push(new NotificationSchemasWithIds(notification, schemasWithIds[index], schemasWithIds[index + 1]));
            index += 2;
        }

        return notificationSchemasWithIds;
    }

    async addRequestSchema(...schemas: Array<Request>): Promise<Array<RequestSchemasWithIds>> {
        const requestSchemasWithIds = [] as Array<RequestSchemasWithIds>;
        const schemaTypes = [] as Array<any>;
        for (const request of schemas) {
            schemaTypes.push(request.request);
            schemaTypes.push(request.response);
        }

        const schemasWithIds = await this.addProducedSchema(schemaTypes);

        let index = 0;
        for (const request of schemas) {
            requestSchemasWithIds.push(new RequestSchemasWithIds(request, schemasWithIds[index], schemasWithIds[index + 1]));
            index += 2;
        }

        return requestSchemasWithIds;
    }
}

export class AvroNotificationProducer {
    private initialized = false;
    private notificationSchemasWithId: NotificationSchemasWithIds;

    constructor(private redisPubSub: RedisPubSub, private avroSchemaCacheManager: AvroSchemasProducedManager,
                private notificationProtocol: NotificationProtocol, private notificationSchema: Notification) {
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            this.notificationSchemasWithId = (await this.avroSchemaCacheManager.addNotificationSchema(this.notificationSchema))[0];
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

export class AvroNotificationConsumer extends EventEmitter {
    private initialized = false;
    private notificationSchemasWithId: NotificationSchemasWithIds;

    constructor(private redisPubSub: RedisPubSub, private avroSchemaCacheManager: AvroSchemasProducedManager,
                private notificationProtocol: NotificationProtocol, private notificationSchema: Notification) {
        super();
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            this.notificationSchemasWithId = (await this.avroSchemaCacheManager.addNotificationSchema(this.notificationSchema))[0];

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

export class RedisAvroMessaging {
    private redisPubSub: RedisPubSub;
    private avroSchemaCacheManager: AvroSchemaCacheManager;
    private avroSchemasProducedManager: AvroSchemasProducedManager;
    private notificationProtocol: NotificationProtocol;

    constructor(redisHost: string, redisPort: number) {
        this.redisPubSub = new RedisPubSub(redisHost, redisPort);
        this.avroSchemaCacheManager = new AvroSchemaCacheManager(this.redisPubSub);
        this.avroSchemasProducedManager = new AvroSchemasProducedManager(this.redisPubSub);
        this.notificationProtocol = new NotificationProtocol(this.avroSchemaCacheManager);
    }

    async createAvroNotificationProducer(notificationSchema: Notification): Promise<AvroNotificationProducer> {
        const avroNotificationProducer = new AvroNotificationProducer(this.redisPubSub, this.avroSchemasProducedManager,
            this.notificationProtocol, notificationSchema);
        await avroNotificationProducer.initialize();

        return avroNotificationProducer;
    }

    async createAvroNotificationConsumer(): Promise<void> {

    }

    async reateAvroRequestClient(): Promise<void> {

    }

    async createAvroRequestHandler(): Promise<void> {

    }
}
