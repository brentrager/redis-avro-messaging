import AvroSchemaWithId from './AvroSchemaWithId';
import { Request } from './types';

export default class RequestSchemasWithIds {
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