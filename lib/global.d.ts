
declare module 'rsmq-promise' {
    import RedisSMQ from 'rsmq';
    class RedisSMQPromise {
        constructor(options: any);
        listQueues(): Promise<Array<string>>;
        createQueue(optionss: RedisSMQ.CreateQueueOptions): Promise<1>;
        setQueueAttributes(options: RedisSMQ.SetQueueAttributesOptions): Promise<RedisSMQ.QueueAttributes>;
        getQueueAttributes(options: RedisSMQ.GetQueueAttributesOptions): Promise<RedisSMQ.QueueAttributes>;
        deleteQueue(options: RedisSMQ.DeleteQueueOptions): Promise<1>;
        sendMessage(options: RedisSMQ.SendMessageOptions): Promise<number>;
        receiveMessage(options: RedisSMQ.ReceiveMessageOptions): Promise<{} | RedisSMQ.QueueMessage>;
        deleteMessage(options: RedisSMQ.DeleteMessageOptions): Promise<0 | 1>;
        popMessage(options: RedisSMQ.PopMessageOptions): Promise<{} | RedisSMQ.QueueMessage>;
    }
}