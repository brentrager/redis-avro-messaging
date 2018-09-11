export default class ExpiringPromise {
    /*
     * Meant to be used as ExpiringPromise.waitWithTimeout(promise, timeoutMs).
     *
     * Constructor is not meant to be called directly.
     */
    private timeoutPromise: Promise<void>;

    constructor(private promise: Promise<any>, private timeoutMs: number = 10000) {
        this.timeoutPromise = new Promise((_resolve, reject) => {
            setTimeout(() => {
                const error = new Error('Promise timed out.');

                return reject(error);
            }, timeoutMs);
        });
    }

    async wait(): Promise<any> {
        return Promise.race([this.promise, this.timeoutPromise]);
    }

    static async waitWithTimeout(promise: Promise<any>, timeoutMs: number = 10000): Promise<any> {
        const expiringPromise = new ExpiringPromise(promise, timeoutMs);

        return expiringPromise.wait();
    }
}