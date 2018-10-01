import Queue from 'p-queue';

export default class Executor {
  constructor() {
    this.buffer = [];
    this.ready = false;
    this.queue = new Queue({ concurrency: 1 });
  }

  push(task, forceQueueing) {
    if (this.ready || forceQueueing) {
      return this.queue.add(task);
    }
    this.buffer.push(task);
  }

  processBuffer() {
    this.ready = true;
    for (let i = 0; i < this.buffer.length; i = i + 1) {
      this.queue.add(this.buffer[i]);
    }
    this.buffer = [];
  }

  onEmpty() {
    return this.queue.onEmpty();
  }
}
