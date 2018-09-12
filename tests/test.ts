import RedisAvroMessaging from '../index';
import { NOTFOUND } from 'dns';

const redisHost = 'localhost';
const redisPort = 6379;

const NOTIFICATION_EMPTY = {
    topic: 'emptyNotification',
    key: {
        name: 'emptyKey',
        type: 'record',
        fields: []
    },
    value: {
        name: 'emptyValue',
        type: 'record',
        fields: []
    }
};

const NOTIFICATION_BIG = {
    topic: 'bigNotification',
    key: {
        name: 'bigKey',
        type: 'record',
        fields: [
            { name: 'keyId', type: 'string' }
        ]
    },
    value: {
        name: 'bigMessage',
        type: 'record',
        fields: [
            {
                name: 'channel', type: {
                    name: 'channel',
                    type: 'record',
                    fields: [
                        {
                            name: 'platform', type: {
                                name: 'platform',
                                type: 'enum',
                                symbols: ['Facebook', 'Twitter']
                            }
                        },
                        { name: 'displayName', type: 'string' }
                    ]
                }
            },
            {
                name: 'data', type: {
                    name: 'message',
                    type: 'record',
                    fields: [
                        { name: 'id', type: 'string' },
                        { name: 'threadId', type: 'string' },
                        {
                            name: 'from', type: {
                                name: 'from',
                                type: 'record',
                                fields: [
                                    { name: 'id', type: 'string' },
                                    { name: 'screenName', type: 'string' },
                                    { name: 'displayName', type: 'string' },
                                    { name: 'pictureUrl', type: 'string' },
                                    { name: 'followers', type: 'int' }
                                ]
                            }
                        },
                        {
                            name: 'content', type: {
                                name: 'content',
                                type: 'record',
                                fields: [
                                    { name: 'time', type: 'long' },
                                    { name: 'text', type: 'string' },
                                    {
                                        name: 'attachments', type: {
                                            type: 'array', items: {
                                                name: 'attachment',
                                                type: 'record',
                                                fields: [
                                                    { name: 'uri', type: 'string' }
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }
};

const NOTIFICATION_NEW = {
    topic: 'newNotification',
    key: {
        name: 'newKey',
        type: 'record',
        fields: [
            { name: 'keyId', type: 'string' }
        ]
    },
    value: {
        name: 'newMessage',
        type: 'record',
        fields: [
            { name: 'id', type: 'string' },
            { name: 'newId', type: 'string', default: 'newId' },
            { name: 'id2', type: 'string' }
        ]
    }
};

const NOTIFICATION_OLD = {
    topic: 'newNotification',
    key: {
        name: 'newKey',
        type: 'record',
        fields: [
            { name: 'keyId', type: 'string' }
        ]
    },
    value: {
        name: 'newMessage',
        type: 'record',
        fields: [
            { name: 'id', type: 'string' },
            { name: 'id2', type: 'string' }
        ]
    }
};

const REQUEST_LIBRARY = {
    requestTopic: 'requestTopic',
    requests: {
        getChildPosts: {
            request: {
                name: 'getChildPosts',
                type: 'record',
                fields: [
                    { name: 'channelId', type: 'string' },
                    { name: 'parentPost', type: 'string' },
                    { name: 'startTime', type: 'long' },
                    { name: 'postLimit', type: 'int' }
                ]
            },
            response: {
                name: 'getChildPostsResponse',
                type: 'record',
                fields: [
                    {
                        name: 'posts', type: {
                            type: 'array', items: {
                                name: 'post',
                                type: 'record',
                                fields: [
                                    { name: 'id', type: 'string' },
                                    { name: 'parentId', type: 'string' },
                                    {
                                        name: 'allow', type: {
                                            name: 'allow', type: 'record', fields: [
                                                { name: 'hide', type: 'boolean' },
                                                { name: 'edit', type: 'boolean' },
                                                { name: 'reply', type: 'boolean' },
                                                { name: 'like', type: 'boolean' }
                                            ]
                                        }
                                    },
                                    {
                                        name: 'from', type: {
                                            name: 'from',
                                            type: 'record',
                                            fields: [
                                                { name: 'id', type: 'string' },
                                                { name: 'screenName', type: 'string' },
                                                { name: 'displayName', type: 'string' },
                                                { name: 'pictureUrl', type: 'string' },
                                                { name: 'followers', type: 'int' }
                                            ]
                                        } },
                                    { name: 'hidden', type: 'boolean' },
                                    { name: 'postTime', type: 'long' },
                                    {
                                        name: 'content', type: {
                                            name: 'content',
                                            type: 'record',
                                            fields: [
                                                { name: 'time', type: 'long' },
                                                { name: 'text', type: 'string' },
                                                {
                                                    name: 'attachments', type: {
                                                        type: 'array', items: {
                                                            name: 'attachment',
                                                            type: 'record',
                                                            fields: [
                                                                { name: 'uri', type: 'string' }
                                                            ]
                                                        } } }
                                            ]
                                        } }
                                ]
                            } } }
                ]
            }
        }
    }
};

let ram: RedisAvroMessaging;
let ram2: RedisAvroMessaging;

describe('Test RedisAvroMessaging', () => {
    beforeAll(async () => {
        ram = new RedisAvroMessaging(redisHost, redisPort);
        ram2 = new RedisAvroMessaging(redisHost, redisPort);
    });

    describe('Test Notfications', () => {
        test('Test with empty schema.', async () => {
            const producer = await ram.createAvroNotificationProducer(NOTIFICATION_EMPTY);
            const consumer = await ram2.createAvroNotificationConsumer(NOTIFICATION_EMPTY);

            const notification = {
                topic: 'big',
                key: {},
                value: {}
            };

            const result = await new Promise(async (resolve) => {

                consumer.on('notification', (key: any, value: any) => {
                    resolve({ key, value });
                });

                await producer.produce(notification.key, notification.value);
            }) as any;

            expect(notification.key).toEqual(result.key);
            expect(notification.value).toEqual(result.value);
        });

        test('Test with big schema.', async () => {
            const producer = await ram.createAvroNotificationProducer(NOTIFICATION_BIG);
            const consumer = await ram2.createAvroNotificationConsumer(NOTIFICATION_BIG);

            const notification = {
                topic: 'empty',
                key: {
                    keyId: 'test'
                },
                value: {
                    channel: {
                        platform: 'Facebook',
                        displayName: 'Facebook channel'
                    },
                    data: {
                        id: 'messageId',
                        threadId: 'threadId',
                        from: {
                            id: 'fromId',
                            screenName: 'fromScreenName',
                            displayName: 'fromDisplayName',
                            pictureUrl: 'fromPictureUrl',
                            followers: 50
                        },
                        content: {
                            time: 12345,
                            text: 'The message.',
                            attachments: [
                                { uri: 'https://google.com' },
                                { uri: 'https://reddit.com' }
                            ]
                        }
                    }
                }
            };

            const result = await new Promise(async (resolve) => {

                consumer.on('notification', (key: any, value: any) => {
                    resolve({ key, value });
                });

                await producer.produce(notification.key, notification.value);
            }) as any;

            expect(notification.key).toEqual(result.key);
            expect(notification.value).toEqual(result.value);
        });

        test('Test producing old schema, consuming new schema.', async () => {
            const producer = await ram.createAvroNotificationProducer(NOTIFICATION_OLD);
            const consumer = await ram2.createAvroNotificationConsumer(NOTIFICATION_NEW);

            const notificationOld = {
                topic: 'big',
                key: {
                    keyId: 'test'
                },
                value: {
                    id: 'id1',
                    id2: 'id2'
                }
            };

            const notificationNew = {
                topic: 'big',
                key: {
                    keyId: 'test'
                },
                value: {
                    id: 'id1',
                    newId: 'newId',
                    id2: 'id2'
                }
            };

            const result = await new Promise(async (resolve) => {

                consumer.on('notification', (key: any, value: any) => {
                    resolve({ key, value });
                });

                await producer.produce(notificationOld.key, notificationOld.value);
            }) as any;

            expect(notificationNew.key).toEqual(result.key);
            expect(notificationNew.value).toEqual(result.value);
        });

        test('Test producing new schema, consuming old schema.', async () => {
            const producer = await ram.createAvroNotificationProducer(NOTIFICATION_NEW);
            const consumer = await ram2.createAvroNotificationConsumer(NOTIFICATION_OLD);

            const notificationOld = {
                topic: 'big',
                key: {
                    keyId: 'test'
                },
                value: {
                    id: 'id1',
                    id2: 'id2'
                }
            };

            const notificationNew = {
                topic: 'big',
                key: {
                    keyId: 'test'
                },
                value: {
                    id: 'id1',
                    newId: 'newId',
                    id2: 'id2'
                }
            };

            const result = await new Promise(async (resolve) => {

                consumer.on('notification', (key: any, value: any) => {
                    resolve({ key, value });
                });

                await producer.produce(notificationNew.key, notificationNew.value);
            }) as any;

            expect(notificationOld.key).toEqual(result.key);
            expect(notificationOld.value).toEqual(result.value);
        });
    });

    describe('Test Requests', () => {
        test('Test Request', async () => {
            const request = {
                channelId: 'channelId',
                parentPost: 'parentPost',
                startTime: 12345,
                postLimit: 1234
            };

            const response = {
                posts:
                [
                    {
                        id: 'id',
                        parentId: 'parentId',
                        allow: {
                            hide: true,
                            edit: false,
                            reply: true,
                            like: false
                        },
                        from: {
                            id: 'id',
                            screenName: 'screenName',
                            displayName: 'displayName',
                            pictureUrl: 'pictureUrl',
                            followers: 42
                        },
                        hidden: true,
                        postTime: 12345,
                        content: {
                            time: 12345,
                            text: 'text',
                            attachments: [
                                { uri: 'uri' }
                            ]
                        }
                    },
                    {
                        id: 'id2',
                        parentId: 'parentId',
                        allow: {
                            hide: true,
                            edit: false,
                            reply: true,
                            like: false
                        },
                        from: {
                            id: 'id',
                            screenName: 'screenName',
                            displayName: 'displayName',
                            pictureUrl: 'pictureUrl',
                            followers: 42
                        },
                        hidden: true,
                        postTime: 12345,
                        content: {
                            time: 12345,
                            text: 'text',
                            attachments: [
                                { uri: 'uri' }
                            ]
                        }
                    }
                ]
            };

            const requestServer = {
                getChildPosts: jest.fn().mockImplementation(async (_request: any) => {
                    return response;
                })
            };

            const avroRequestClient = await ram.createAvroRequestClient();
            const avroRequestHandler = await ram2.createAvroRequestHandler(requestServer, REQUEST_LIBRARY);

            const responseReceived = await avroRequestClient.sendRequest(REQUEST_LIBRARY.requestTopic, REQUEST_LIBRARY.requests.getChildPosts, request);

            expect(requestServer.getChildPosts).toBeCalledWith(request);
            expect(responseReceived).toEqual(response);
        });
    });
});