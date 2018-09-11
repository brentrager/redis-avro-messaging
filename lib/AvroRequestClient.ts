import RedisPubSub from './RedisPubSub';
import { AVRO_REQUEST_CHANNEL, AVRO_RESPONSE_CHANNEL, NODE_ID } from './constants';
import { ChannelMessage, Request, Message } from './types';
import AvroRequestProtocol from './AvroRequestProtocol';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
import * as uuid from 'uuid';
import AvroSchemaWithId from './AvroSchemaWithId';
// tslint:disable-next-line:variable-name no-require-imports
const icAvroLib = require('@pureconnect/icavrolib');
// tslint:disable-next-line:variable-name no-require-imports
const ContainerLogging = require('@pureconnect/containerlogging');
// tslint:disable-next-line:variable-name no-require-imports
const requestHeader = require('../schemas/requestheader.json');
// tslint:disable-next-line:variable-name no-require-imports
const responseHeader = require('../schemas/responseheader.json');
// tslint:disable-next-line:variable-name no-require-imports
const responseError = require('../schemas/responseerror.json');

const log = new ContainerLogging('redis-avro-messaging', 'AvroRequestClient');

interface LocalSchemasWithIds {
    requestHeader: AvroSchemaWithId;
    responseHeader: AvroSchemaWithId;
    responseError: AvroSchemaWithId;
}

export default class AvroRequestClient {
    private initialized = false;
    private localSchemasWithIds: LocalSchemasWithIds;
    private responseTopic: string;

    // Default timeout for responses to requests
    private defaultRequestTimeoutMs = 10000;

    // Map of correlation IDs to the requests' Promise resolve/reject functions.
    // Keys are strings used as correlation IDs (uuidv4 strings).
    // Values are objects with 'resolve' = Promise's resolve function, 'reject' = Promise's reject
    // function.
    // These requests resolve with AvroReceivePayloads containing the response body payload.
    private pendingRequests = new Map();

    constructor(private redisPubSub: RedisPubSub, private avroSchemasProducedManager: AvroSchemasProducedManager,
                private requestProtocol: AvroRequestProtocol) {
        this.responseTopic = NODE_ID;
    }

    async initialize(): Promise<void> {
        if (!this.initialized) {
            const schemasWithIds = await this.avroSchemasProducedManager.addProducedSchema(responseHeader, responseError, requestHeader);
            this.localSchemasWithIds = {
                responseHeader: schemasWithIds[0],
                responseError: schemasWithIds[1],
                requestHeader: schemasWithIds[2]
            };

            this.redisPubSub.on('message', async (channelMessage: ChannelMessage) => {
                try {
                    if (channelMessage.channel === AVRO_RESPONSE_CHANNEL) {
                        const nodeId = channelMessage.message.nodeId;
                        const requestPayload = channelMessage.message.message as Request;

                        if (this.responseTopic === requestPayload.responseTopic) {
                            if (!requestPayload.response || !requestPayload.response.length) {
                                log.info(`Received ping on ${this.responseTopic}`);

                                return;
                            }

                            log.verbose(`Received ${requestPayload.response.length}-byte message on ${this.responseTopic}`);

                            const payloads = await this.requestProtocol.parseMessage(requestPayload.response, nodeId);

                            // Read the header
                            const header = payloads.headerPayload.deserialize(responseHeader);
                            // Find the pending request
                            const request = this.pendingRequests.get(header.correlationId);
                            // Unknown correlation ID.  The request might have timed out and been removed.
                            if (!request) {
                                log.warn(`Received response to ${header.correlationId},
                                    which was not found.  Response type is ${payloads.bodyPayload.getSchemaName()}`);

                                return;
                            }

                            this.pendingRequests.delete(header.correlationId);
                            request.resolve(payloads.bodyPayload);
                        }
                    }
                } catch (error) {
                    log.error(`Error getting message on ${this.responseTopic}: ${error}`);
                }
            });

            await this.redisPubSub.subscribe(AVRO_RESPONSE_CHANNEL);

            this.initialized = true;
        }
    }

    /**
     * A request timed out - used by sendRequest().
     */
    requestTimedOut(requestName: string, correlationId: string, timeoutMs: number): void {
        // Find the pending request
        const request = this.pendingRequests.get(correlationId);
        // If the request is already gone, its completion raced with the timeout.  This is fine,
        // nothing to do.
        if (!request) {
            log.info(`Timeout for ${requestName} with ID ${correlationId} fired, but the request already completed.`);

            return;
        }

        this.pendingRequests.delete(correlationId);
        // Reject the promise with an error.
        request.reject(new Error(`Request for ${requestName} with ID ${correlationId} timed out after ${timeoutMs} ms`));
    }

    /**
     * Send a request.
     *
     * @param requestTopic - The topic name where the request will be published.  This should be
     *       provided in some way by the microservice receiving the request.
     * @param requestSchemas - An object identifying the request and response schemas.  Its
     *       'request' and 'response' members must be the parsed Avro schema objects used to write
     *       the request and read the response.
     *       This is normally provided by the microservice that services the request, such as
     *       require('cloudconn-schemas/create-post.json').
     * @param body - The body of the request.
     * @param timeoutMs - Timeout for the request in ms.  Defaults to 10 s.  This includes both the
     *       time for the schema registration request (if needed) and the actual request to the
     *       server.
     * @return Promise - the response data (deserialized with the response schema given)
     */
    async sendRequest(requestTopic: string, requestSchemas: Request, body: Request, timeoutMs: number = this.defaultRequestTimeoutMs): Promise<any> {
        // Get the parsed type for the request schema.
        const requestType = icAvroLib.getParsedType(requestSchemas.request);
        // Generate a correlation ID.
        const correlationId = uuid.v4();
        log.verbose(`Sending request for ${requestType.getName()} to ${requestTopic} with correlation ID ${correlationId}`);

        // Create a promise for the request and store it
        const requestPromise = new Promise((resolve, reject) => {
            this.pendingRequests.set(correlationId, { resolve, reject });
        });
        // Create a timeout for this request
        const timeoutId = setTimeout(() => {
            this.requestTimedOut(requestType.getName(), correlationId, timeoutMs);
        }, timeoutMs);

        try {
            // Get the registered schema ID for the request schema.  The subject name is composed from
            // the request topic and schema name, since the schema name is used to determine the type of
            // request by the recipient.
            const avroRequestSchemasWithIds = await this.avroSchemasProducedManager.addRequestSchema(requestSchemas);
            const requestSchema = avroRequestSchemasWithIds[0];

            // If the request already timed out, don't produce anything.  The caller's promise
            // has already been rejected by the timeout, we just skip the request production.
            if (!this.pendingRequests.has(correlationId)) {
                log.info(`Request for ${requestType.getName()} with ID ${correlationId} timed out before request was produced`);
            } else {
                // Build the request payload.
                const header = {
                    responseTopic: this.responseTopic,
                    correlationId,
                    expireTimestamp: Date.now() + timeoutMs
                };

                const messageBufString = this.requestProtocol.buildMessage(icAvroLib.getParsedType(this.localSchemasWithIds.requestHeader.schema()),
                                                            this.localSchemasWithIds.requestHeader.id(),
                                                            header,
                                                            requestSchema.request().schema(),
                                                            requestSchema.request().id(),
                                                            body);

                log.verbose(`Sending ${messageBufString.length}-byte message for
                    ${requestType.getName()} to ${requestTopic} with correlation ID ${correlationId}`);
                // Send the request
                const request: Request = {
                    requestTopic,
                    request: messageBufString
                };

                await this.redisPubSub.publish(AVRO_REQUEST_CHANNEL, request);
            }

        } catch (error) {
            // Sending the request failed - could be a failure to register the schema, or the
            // message could not be serialized, etc.
            // It's possible that the request could already have timed out
            const request = this.pendingRequests.get(correlationId);
            if (!request) {
                log.info(`Request for ${requestType.getName()} with ID ${correlationId} failed, but had already timed out.`, error);

                return;
            }

            clearTimeout(timeoutId);
            this.pendingRequests.delete(correlationId);

            throw error;
        }

        // After the response is received, deserialize it using the desired schema.
        return requestPromise.then((responsePayload: any) => {
            // Delete the timeout, it's no longer needed
            clearTimeout(timeoutId);
            // If it's an error, read it and throw it
            if (responsePayload.getSchemaName() === responseError.name) {
                const errorResponse = responsePayload.deserialize(responseError);
                log.warn(`Request for ${requestType.getName()} with ID ${correlationId} resulted in error: ${errorResponse.error}`);
                // Throw the error to fulfill the promise with the error
                throw new Error(errorResponse.error);
            }

            // It's successful, read the result and return it to fulfill the promise
            return responsePayload.deserialize(requestSchemas.response);
        });
    }
}