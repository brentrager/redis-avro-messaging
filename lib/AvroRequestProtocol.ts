import AvroSchemaCacheManager from './AvroSchemaCacheManager';
// tslint:disable-next-line:variable-name no-require-imports
const icAvroLib = require('@pureconnect/icavrolib');

/**
 * AvroRequestProtocol builds and parses messages using the internal request-response wire protocol.
 *
 * Requests and responses are serializing using multiple Avro-encoded parts.  The parts are
 * separated based on who controls the schema used to encode them.
 *
 * Offset        | Length      | Data
 * 0             | 4           | Header schema ID, little-endian (from Confluent registry)
 * 4             | 4           | (header_len) Header data length, little endian
 * 8             | 4           | Body schema ID, little-endian (from Confluent registry)
 * 12            | 4           | (body_len) Body data length, little endian
 * 16            | header_len  | Header data
 * 16+header_len | body_len    | Body data
 *
 * Note that the wire protocol allows either data section to be 0 bytes long (although the schemas
 * used may require a minimum length).
 *
 * In addition, zero-length messages are allowed and used as pings to keep the response topic alive.
 *
 * The 'header' section is data controlled by this library itself.  The schemas for this are an
 * internal part of this library.  This contains the data needed to route the response.  For a
 * request, it includes a correlation ID and response topic name.  For a response, it only includes
 * the correlation ID.  These data are not exposed to either micorservice; they're internal to this
 * library.
 *
 * The 'body' section contains the actual request or response data - these schemas are controlled
 * by the microservice that services the request.
 *
 * NOTE: We could consider adding a 'roundtrip' payload section also that would contain optional
 * data that's roundtripped back to the microservice that created the request.  This schema would be
 * defined by the requesting microservice.  This might be helpful to reduce state in the requesting
 * microservice, or to allow a microservice to consume responses as a consumer group instead of
 * requiring them to go to the instance that created the request.
 */

export default class AvroRequestProtocol {
    private protocolVersion = 0;
    private offsets = {
        headerSchemaId: 0,
        headerDataLength: 4,
        bodySchemaId: 8,
        bodyDataLength: 12,
        data: 16
    };

    constructor(private avroSchemaCacheManager: AvroSchemaCacheManager) {
    }

    /**
     * Build a message from payload parts and their avroJs Types.
     *
     * @param headerType - The avroJs Type to use to encode the header.
     * @param headerSchemaId - The schema registry's ID for the header schema in headerType.
     * @param header - The header data.
     * @param bodyType - The avroJs Type to use to encode the body.
     * @param bodySchemaId = The schema registry's ID for the body scheam in bodyType.
     * @param body - The body data.
     * @return A Buffer containing the message built from those parts.
     */
    buildMessage(headerType: any, headerSchemaId: number, header: any, bodyType: any, bodySchemaId: number, body: any): string {
        const headerLen = icAvroLib.getAvroLength(headerType, header);
        const bodyLen = icAvroLib.getAvroLength(bodyType, body);
        const messageBuf = Buffer.allocUnsafe(this.offsets.data + headerLen + bodyLen);

        // Write the schema IDs and payload lengths
        messageBuf.writeUInt32LE(headerSchemaId, this.offsets.headerSchemaId);
        messageBuf.writeUInt32LE(headerLen, this.offsets.headerDataLength);
        messageBuf.writeUInt32LE(bodySchemaId, this.offsets.bodySchemaId);
        messageBuf.writeUInt32LE(bodyLen, this.offsets.bodyDataLength);

        // Serialize the header
        icAvroLib.avroSerialize(headerType, messageBuf, this.offsets.data, headerLen, header);
        // Serialize the body
        icAvroLib.avroSerialize(bodyType, messageBuf, this.offsets.data + headerLen, bodyLen, body);

        return messageBuf.toString('base64');
    }

    /**
     * Parse a message into its Avro-encoded parts.
     *
     * An incoming message is parsed to split up the header and body parts, then the writer's schemas
     * are fetched from schemaCache.
     *
     * @param messageBufString - The message in base64 containing the data
     * @param nodeId - The node ID of the writer.
     * @return Promise - a promise resolving to a dictionary of two AvroReceivePayloads - the header payload
     *       and the body payload.  If any part of the message is invalid, or if one of the schemas
     *       can't be loaded, the promise is rejected.
     */
    async parseMessage(messageBufString: string, nodeId: string): Promise<any> {
        const messageBuf = Buffer.from(messageBufString, 'base64');

        // Invalid messages should never occur.  Log as warning (not error) because this doesn't
        // indicate a problem in this microservice, it's a problem in the other microservice.
        if (messageBuf.length < this.offsets.data) {
            return Promise.reject(`Message size ${messageBuf.length} is invalid`);
        }

        // Read the schema IDs and payload lengths
        const headerSchemaId = messageBuf.readUInt32LE(this.offsets.headerSchemaId);
        const headerDataLength = messageBuf.readUInt32LE(this.offsets.headerDataLength);
        const bodySchemaId = messageBuf.readUInt32LE(this.offsets.bodySchemaId);
        const bodyDataLength = messageBuf.readUInt32LE(this.offsets.bodyDataLength);

        // Validate the data lengths
        if (messageBuf.length < this.offsets.data + headerDataLength + bodyDataLength) {
            return Promise.reject(`Message size ${messageBuf.length} is invalid for header length ${headerDataLength} and body length ${bodyDataLength}`);
        }

        // Slice off the data sections
        const headerData = messageBuf.slice(this.offsets.data, this.offsets.data + headerDataLength);
        const bodyData = messageBuf.slice(this.offsets.data + headerDataLength,
            this.offsets.data + headerDataLength + bodyDataLength);

        // Get the schemas and create AvroReceivePayloads
        const headerSchema = await this.avroSchemaCacheManager.getSchema(nodeId, headerSchemaId);
        const headerType = icAvroLib.getParsedType(headerSchema.schema());
        const headerPayload = new icAvroLib.AvroReceivePayload(headerData, headerType);

        const bodySchema = await this.avroSchemaCacheManager.getSchema(nodeId, bodySchemaId);
        const bodyType = icAvroLib.getParsedType(bodySchema.schema());
        const bodyPayload = new icAvroLib.AvroReceivePayload(bodyData, bodyType);

        return { headerPayload, bodyPayload };
    }
}