export class MessageHub {
  private listenerLookup: Record<string, Array<Function>> = {};

  subscribe(key: string, listener: Function) {
    if (!this.listenerLookup[key]) {
      this.listenerLookup[key] = [];
    }

    this.listenerLookup[key].push(listener);
    return () => this.unsubscribe(key, listener);
  }

  unsubscribe(key: string, listener: Function) {
    const listeners = this.listenerLookup[key];
    if (!listeners) {
      return;
    }

    const index = listeners.indexOf(listener);
    if (index > -1) {
      listeners.splice(index, 1);

      if (!listeners.length) {
        delete this.listenerLookup[key];
      }
    }
  }

  publish(key: string, ...params: Array<any>) {
    if (this.listenerLookup[key]) {
      return Promise.all(
        this.listenerLookup[key].map((listener) => listener(...params)),
      );
    }
  }
}

export default MessageHub;
