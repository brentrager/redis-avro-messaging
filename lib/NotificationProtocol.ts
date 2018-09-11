import AvroSchemaCacheManager from './AvroSchemaCacheManager';
// tslint:disable-next-line:variable-name no-require-imports
const icAvroLib = require('@pureconnect/icavrolib');

export default class NotificationProtocol {
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