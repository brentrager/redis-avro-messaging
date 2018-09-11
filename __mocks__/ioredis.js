const { EventEmitter } = require('events');

class RedisMock extends EventEmitter {
    constructor() {
        super();
    }

    publish(channel, message) {
        this.emit('message', channel, message);
    }
}

const redisMock = new RedisMock();

module.exports = exports = class extends EventEmitter {
    constructor(_port, _host, _options) {
        super();
        redisMock.on('message', (channel, message) => {
            if (this.channelsSet.has(channel)) {
                this.emit('message', channel, message);
            }
        })

        this.channelsSet = new Set();
    }

    subscribe(channels, callback) {
        for (const channel of channels) {
            this.channelsSet.add(channel);
        }
        callback(null, channels.length);
    }

    unsubscribe(channel) {
        this.channelsSet.delete(channel);
    }

    publish(channel, message) {
        redisMock.publish(channel, message);
    }
};