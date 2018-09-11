import RedisAvroMessaging from '../index';

const redisHost = 'localhost';
const redisPort = 6379;
const redisAvroMessaging = new RedisAvroMessaging(redisHost, redisPort);

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

describe('Test with Redis', () => {

});