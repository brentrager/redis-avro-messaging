import AvroSchemaWithId from './AvroSchemaWithId';
import { AVRO_SCHEMA_REGISTER_CHANNEL, AVRO_SCHEMA_REGISTER_REQUEST_CHANNEL, NODE_ID } from './constants';
import RedisPubSub from './RedisPubSub';
import { ChannelMessage, Notification, Request } from './types';
import * as _ from 'lodash';
import AvroNotificationSchemasWithIds from './AvroNotificationSchemasWithIds';
import AvroRequestSchemasWithIds from './AvroRequestSchemasWithIds';
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');

const log = new ContainerLogging('redis-avro-messaging', 'AvroSchemasProducedManager');

export default class AvroSchemasProducedManager {
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
                if (nodeId === NODE_ID) {
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

    async addProducedSchema(...schemas: Array<any>): Promise<Array<AvroSchemaWithId>> {
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
        this.schemasProduced = this.schemasProduced.concat(schemasWithId);

        await this.notifySchemasProduced();

        // Return an array of schema IDs that represent the newly added schemas
        return existingSchemasWithId.concat(schemasWithId);
    }

    async addNotificationSchema(...schemas: Array<Notification>): Promise<Array<AvroNotificationSchemasWithIds>> {
        const notificationSchemasWithIds = [] as Array<AvroNotificationSchemasWithIds>;
        const schemaTypes = [] as Array<any>;
        for (const notification of schemas) {
            schemaTypes.push(notification.key);
            schemaTypes.push(notification.value);
        }

        const schemasWithIds = await this.addProducedSchema(...schemaTypes);

        let index = 0;
        for (const notification of schemas) {
            notificationSchemasWithIds.push(new AvroNotificationSchemasWithIds(notification, schemasWithIds[index], schemasWithIds[index + 1]));
            index += 2;
        }

        return notificationSchemasWithIds;
    }

    async addRequestSchema(...schemas: Array<Request>): Promise<Array<AvroRequestSchemasWithIds>> {
        const requestSchemasWithIds = [] as Array<AvroRequestSchemasWithIds>;
        const schemaTypes = [] as Array<any>;
        for (const request of schemas) {
            schemaTypes.push(request.request);
            schemaTypes.push(request.response);
        }

        const schemasWithIds = await this.addProducedSchema(...schemaTypes);

        let index = 0;
        for (const request of schemas) {
            requestSchemasWithIds.push(new AvroRequestSchemasWithIds(request, schemasWithIds[index], schemasWithIds[index + 1]));
            index += 2;
        }

        return requestSchemasWithIds;
    }
}