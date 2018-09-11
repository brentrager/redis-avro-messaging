export interface Message {
    nodeId: string;
    message: any;
}

export interface ChannelMessage {
    channel: string;
    message: Message;
}

export interface Notification {
    topic: string;
    key: any;
    value: any;
}

export interface Request {
    topic: string;
    request?: any;
    response?: any;
}