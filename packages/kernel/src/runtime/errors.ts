export class TaskCancelledError extends Error {
  constructor(readonly taskId: string, readonly reason?: unknown) {
    super(`Task cancelled: ${taskId}`, { cause: reason });
    this.name = 'TaskCancelledError';
  }
}

export class EngineDestroyedError extends Error {
  constructor() {
    super('Task runtime has been destroyed');
    this.name = 'EngineDestroyedError';
  }
}

export class TransactionRejectedError extends Error {
  constructor(readonly operation: string) {
    super(`Transaction rejected during ${operation}`);
    this.name = 'TransactionRejectedError';
  }
}
