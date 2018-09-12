import { EventEmitter } from 'events';
import RedisPubSub from './RedisPubSub';
import { AVRO_REQUEST_CHANNEL, AVRO_RESPONSE_CHANNEL } from './constants';
import { ChannelMessage, Request } from './types';
import AvroRequestProtocol from './AvroRequestProtocol';
import AvroSchemasProducedManager from './AvroSchemasProducedManager';
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

const log = new ContainerLogging('redis-avro-messaging', 'AvroRequestHandler');

interface LocalSchemasWithIds {
    requestHeader: AvroSchemaWithId;
    responseHeader: AvroSchemaWithId;
    responseError: AvroSchemaWithId;
}

export interface ServiceObject {
    [func: string]: Function;
}

export interface ServiceDefinition {
    requestTopic: string;
    requests: {
        [schemaName: string]: any;
    };
}

export class AvroRequestHandler extends EventEmitter {
    private initialized = false;
    private localSchemasWithIds: LocalSchemasWithIds;
    private requestTopic: string;
    private requestsByType: Map<string, any>;

    constructor(private redisPubSub: RedisPubSub, private avroSchemasProducedManager: AvroSchemasProducedManager,
                private requestProtocol: AvroRequestProtocol, private serviceObj: ServiceObject, private serviceDef: ServiceDefinition) {
        super();
        this.requestTopic = this.serviceDef.requestTopic;

        // Put all the requests in a Map to look them up by type name.
        this.requestsByType = new Map();
        for (const requestName in serviceDef.requests) {
            if (serviceDef.requests.hasOwnProperty(requestName)) {
                const requestDef = serviceDef.requests[requestName];
                this.requestsByType.set(requestDef.request.name, { name: requestName, def: requestDef });
            }
        }
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
                    if (channelMessage.channel === AVRO_REQUEST_CHANNEL) {
                        const nodeId = channelMessage.message.nodeId;
                        const requestPayload = channelMessage.message.message as Request;

                        if (this.requestTopic === requestPayload.requestTopic) {
                            if (!requestPayload.request || !requestPayload.request.length) {
                                log.info(`Received ping on ${this.requestTopic}`);

                                return;
                            }

                            log.verbose(`Received ${requestPayload.request.length}-byte message on ${this.requestTopic}`);

                            const payloads = await this.requestProtocol.parseMessage(requestPayload.request, nodeId);

                            // Read the header
                            const header = payloads.headerPayload.deserialize(requestHeader);

                            // If the request already expired, quit without delivering it to the application.
                            // The client has already given up on this request.
                            // Timing out this way is important in case a microservice would be down for an
                            // extended period of time, or if a burst of bad requests causes it to restart
                            // repeatedly, etc. - this ensures that we'll eventually ignore the burst of traffic
                            // instead of getting stuck processing it for an extended period of time.
                            // (Otherwise, since the requests remain in the Kafka queue even if the microservice
                            // restarts, we would keep restarting and keep trying to service these requests that
                            // have already timed out.)
                            const now = Date.now();
                            if (now >= header.expireTimestamp) {
                                log.info(`Ignore request on ${this.requestTopic} with type ${payloads.bodyPayload.getSchemaName()}
                                    and correlation ID ${header.correlationId}; it timed out ${now - header.expireTimestamp} ms ago.`);

                                return;
                            }

                            log.verbose(`Received request on ${this.requestTopic}
                                with type ${payloads.bodyPayload.getSchemaName()} and correlation ID ${header.correlationId}`);

                            // Define the response function
                            const respond = async (bodyOrError: any, successSchema: any) => {
                                try {
                                    await this.sendResponse(bodyOrError, header, successSchema);
                                } catch (error) {
                                    log.error(`Error calling response function: ${error}`);
                                }
                            };

                            /**
                             * Request event.
                             *
                             * @event KafkaRequestServer#request
                             * @param bodyPayload - An AvroReceivePayload containing the request data.  Examine
                             *       its type with bodyPayload.getSchemaName() to determine the type of request,
                             *       then deserialize it with bodyPayload.deserialize().
                             * @param respond(body, schema) - A function that will respond to the event, call
                             *       this when the response is ready.  For a sucessful result, provide the
                             *       response data object and the Avro schema object to use to serialize it.
                             *       For an error result, provide an Error object as the body.  schema is
                             *       ignored in this case; an internal error schema is used to send the error
                             *       message.
                             */
                            this.emit('request', payloads.bodyPayload, respond);
                        }
                    }
                } catch (error) {
                    log.error(`Error getting message on ${this.requestTopic}: ${error}`);
                }
            });

            await this.redisPubSub.subscribe(AVRO_REQUEST_CHANNEL);

            this.on('request', (payload, respond) => this.handleRequest(payload, respond));

            this.initialized = true;
        }
    }

    /**
     * Send a response to a message.  This isn't intended to be used directly; it's part of the
     * implementation of the 'respond' function provided with the 'request' event.
     *
     * @param bodyOrError - The body for the response if successful, or an Error object if not.
     * @param requestHeader - The header from the request
     * @param responseSchema - The schema object to use to serialize the response if it was
     *       successful.  This is ignored if bodyOrError is an Error.
     */
    async sendResponse(bodyOrError: any, requestHeaderData: any, responseSchema: any): Promise<void> {
        if (bodyOrError instanceof Error) {
            await this.sendErrorResponse(bodyOrError, requestHeaderData);
        }
        else {
            await this.sendSuccessResponse(requestHeaderData, bodyOrError, responseSchema);
        }
    }

    /**
     * Send an error response to a message.  This is called by sendResponse if the response is an
     * Error.
     */
    async sendErrorResponse(error: Error, requestHeaderData: any): Promise<void> {
        log.warn(`Request ${requestHeaderData.correlationId} resulted in error:`, error);
        // Build the response payload
        const header = { correlationId: requestHeaderData.correlationId};
        const messageBufString = this.requestProtocol.buildMessage(icAvroLib.getParsedType(this.localSchemasWithIds.responseHeader.schema()),
            this.localSchemasWithIds.responseHeader.id(), header, icAvroLib.getParsedType(this.localSchemasWithIds.responseError.schema()),
            this.localSchemasWithIds.responseError.id(), {error: error.message});

        log.verbose(`Sending ${messageBufString.length}-byte error message to
            ${requestHeaderData.responseTopic} with correlation ID ${requestHeaderData.correlationId}`);

        // Send the response
        const response: Request = {
            responseTopic: requestHeaderData.responseTopic,
            response: messageBufString
        };
        await this.redisPubSub.publish(AVRO_RESPONSE_CHANNEL, response);
    }

    /**
     * Send a successful response to a message.  This is called by sendResponse if the response is
     * successful.
     *
     * @param requestHeader - The header from the request
     * @param responseBody - The body for the response
     * @param responseSchema - The schema object to use to serialize the response
     */
    async sendSuccessResponse(requestHeaderData: any, responseBody: any, responseSchema: any): Promise<void> {
        // Get the parsed type for the response schema
        const responseType = icAvroLib.getParsedType(responseSchema);

        log.verbose(`Sending response for ${responseType.getName()} to
            ${requestHeaderData.responseTopic} with correlation ID ${requestHeaderData.correlationId}`);

        try {
            // Get the registered schema ID for the response schema.  Like requests, the response subject
            // name is composed using the topic name and the schema name values from the *request*.  The
            // client chooses the schema to deserialize with based on those values - the client is not
            // expected to inspect the response schema's name.
            const responseSchemaWithId = (await this.avroSchemasProducedManager.addProducedSchema(responseSchema))[0];

            // Build the response payload
            const header = { correlationId: requestHeaderData.correlationId };
            const messageBufString = this.requestProtocol.buildMessage(icAvroLib.getParsedType(this.localSchemasWithIds.responseHeader.schema()),
                this.localSchemasWithIds.responseHeader.id(), header, icAvroLib.getParsedType(responseSchemaWithId.schema()), responseSchemaWithId.id(),
                responseBody);

            log.verbose(`Sending ${messageBufString.length}-byte message for ${responseType.getName()} to
                ${requestHeaderData.responseTopic} with correlation ID ${requestHeaderData.correlationId}`);

            // Send the response
            const response: Request = {
                responseTopic: requestHeaderData.responseTopic,
                response: messageBufString
            };
            await this.redisPubSub.publish(AVRO_RESPONSE_CHANNEL, response);
        } catch (error) {
            // If we weren't able to send the successful result (such as if the result didn't
            // conform to the schema, or if the schema couldn't be registered, etc.), send an
            // error instead so the client isn't left to time out.
            await this.sendErrorResponse(error, requestHeaderData);
        }
    }

    /**
     * Fulfill a request by deserializing it, calling the appropriate response handler, then
     * responding with the value produces.
     */
    fulfillRequest(payload: any, requestName: string, requestDef: any, respond: Function): void {
        try {
            const request = payload.deserialize(requestDef.request);

            // Call the implementation method with serviceObj as 'this', pass the request as an
            // argument.
            this.serviceObj[requestName].call(this.serviceObj, request)
                .then((response: any) => respond(response, requestDef.response))
                .catch((error: Error) => {
                    // Respond for async errors from the implementation function
                    log.warn(`Request for ${requestDef.request.name} resulted in error:`, error);
                    respond(error);
                });
        }
        catch (error) {
            // Respond for synchronous errors, such as failure to deserialize the request
            log.warn(`Request for ${requestDef.request.name} resulted in error:`, error);
            respond(error);
        }
    }

    /**
     * Handle an incoming request - find the appropriate handler, then use it to fulfill the
     * request.
     */
    handleRequest(payload: any, respond: Function): void {
        const requestEntry = this.requestsByType.get(payload.getSchemaName());
        if (requestEntry) {
            this.fulfillRequest(payload, requestEntry.name, requestEntry.def, respond);
        } else {
            const unknownReqErr = new Error(`Unknown request: ${payload.getSchemaName()}`);
            log.warn(unknownReqErr);
            respond(unknownReqErr);
        }
    }
}