import AvroSchemaWithId from './AvroSchemaWithId';
import { Notification } from './types';

export default class NotificationSchemasWithIds {
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