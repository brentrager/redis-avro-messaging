export default class AvroSchemaWithId {
    constructor(private _id: number, private _schema: any) {
    }

    schema(): any {
        return this._schema;
    }

    id(): number {
        return this._id;
    }
}