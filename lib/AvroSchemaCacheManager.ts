import { EventEmitter } from 'events';
import AvroSchemaCache from './AvroSchemaCache';
import RedisPubSub from './RedisPubSub';
import ExpiringPromise from './ExpiringPromise';
import AvroSchemaWithId from './AvroSchemaWithId';
import { AVRO_SCHEMA_REGISTER_CHANNEL, AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL, AVRO_SCHEMA_REGISTER_REQUEST_BACKUP_CHANNEL } from './constants';
import { ChannelMessage } from './types';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');

const log = new ContainerLogging('redis-avro-messaging', 'AvroSchemaCacheManager');

export default class AvroSchemaCacheManager extends EventEmitter {
    private schemaCache: AvroSchemaCache;
    /*
     * This is a wrapper around AvroSchemaCache which proactively requests missing schemas from other microservices.
     * If this microservice started after another, it may be missing some of the other microservices schemas. If we
     * need a schema and don't have it, this class will request it.
     */
    constructor(private redisPubSub: RedisPubSub) {
        super();
        this.schemaCache = new AvroSchemaCache();

        this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
            if (channelMessage.channel === AVRO_SCHEMA_REGISTER_CHANNEL) {
                const message = channelMessage.message;
                const nodeId = message.nodeId;
                const schemas = message.message.schemas as Array<any>;
                this.schemaCache.setSchemas(nodeId, schemas);
                this.emit('schemasRegistered', { nodeId, schemas });
            } else if (channelMessage.channel === AVRO_SCHEMA_REGISTER_REQUEST_BACKUP_CHANNEL) {
                const nodeId = channelMessage.message.message.nodeId;
                const schemas = this.schemaCache.getSchemas(nodeId);
                if (schemas && schemas.length) {
                    await this.redisPubSub.publish(AVRO_SCHEMA_REGISTER_CHANNEL, { schemas }, nodeId);
                }
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

    /**
     * Return a promise which gets notified if schemas for the node we need are registered.
     *
     * @param nodeId node for which we need schemas
     * @param schemaId schema ID we need
     * @param backup If true, we ask all microservices to see if they have schemas for this node (the original node may have died)
     */
    private async getSchemasRegisteredPromise(nodeId: string, schemaId: number, backup = false): Promise<AvroSchemaWithId> {
        return new Promise<AvroSchemaWithId>(async (resolve, reject) => {
            this.on('schemasRegistered', schemasRegistered => {
                if (schemasRegistered.nodeId === nodeId) {
                    const innerResult = this.schemaCache.getSchema(nodeId, schemaId);

                    if (!innerResult) {
                        const error = new Error(`Schema ${schemaId} does not exist for node ${nodeId}.`);
                        log.error(error);

                        return reject(error);
                    }

                    return resolve(innerResult);
                }
            });

            await this.redisPubSub.publish((!backup ? AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL : AVRO_SCHEMA_REGISTER_REQUEST_BACKUP_CHANNEL), { nodeId });
        });
    }

    async getSchema(nodeId: string, schemaId: number): Promise<AvroSchemaWithId> {
        let result = this.schemaCache.getSchema(nodeId, schemaId);

        if (!result) {
            try {
                result = await ExpiringPromise.waitWithTimeout(this.getSchemasRegisteredPromise(nodeId, schemaId));
            } catch (error) {
                log.error(`Error getting schema ${schemaId} for node ${nodeId}. Trying again from other microservices.`);

                try {
                    result = await ExpiringPromise.waitWithTimeout(this.getSchemasRegisteredPromise(nodeId, schemaId));
                } catch (error) {
                    log.error(`Error getting schema ${schemaId} for node ${nodeId}. Trying again from other microservices.`);
                }
            }
        }

        return result;
    }
}