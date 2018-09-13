class RedisSMQPromise {

    constructor(options) {
        this.queues = new Map();
    };

    async listQueues() {
        return Array.from(this.queues.keys());
    };

    async createQueue(options) {
        return 1;
    };

    async setQueueAttributes(options) {
        return {};
    };

    async getQueueAttributes(options) {
        return {};
    };

    async deleteQueue(options) {
        return 1;
    };

    async sendMessage(options) {
        if (!this.queues.has(options.qname)) {
            this.queues.set(options.qname, []);
        }
        const queue = this.queues.get(options.qname);
        queue.push(options.message);
        return 1;
    };

    async receiveMessage(options) {
        let message = {};
        if (this.queues.has(options.qname)) {
            const queue = this.queues.get(options.qname);
            if (queue.length) {
                const _message = queue.pop();
                message = {
                    message: _message,
                    id: '1',
                    sent: 123,
                    fr: 123,
                    rc: 1
                }
            }
        }
        return message;
    };

    async deleteMessage(options) {
        return 1;
    };

    async popMessage(options) {
        let message = {};
        if (this.queues.has(options.qname)) {
            const queue = this.queues.get(options.qname);
            if (queue.length) {
                const _message = queue.pop();
                message = {
                    message: _message,
                    id: '1',
                    sent: 123,
                    fr: 123,
                    rc: 1
                }
            }
        }
        return message;
    };
}


module.exports = RedisSMQPromise ;
