import AvroSchemaWithId from './AvroSchemaWithId';

export default class AvroSchemaCache {
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