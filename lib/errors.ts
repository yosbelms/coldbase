export class MiniDbError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'MiniDbError'
  }
}

export class PreconditionFailedError extends MiniDbError {
  constructor(message = 'Precondition failed') {
    super(message)
    this.name = 'PreconditionFailedError'
  }
}

export class LockError extends MiniDbError {
  constructor(message: string) {
    super(message)
    this.name = 'LockError'
  }
}

export class LockActiveError extends LockError {
  constructor(collection: string) {
    super(`Lock is active for collection: ${collection}`)
    this.name = 'LockActiveError'
  }
}

export class LockAcquisitionError extends LockError {
  readonly cause?: Error

  constructor(collection: string, cause?: Error) {
    super(`Failed to acquire lock for collection: ${collection}`)
    this.name = 'LockAcquisitionError'
    this.cause = cause
  }
}

export class NotFoundError extends MiniDbError {
  constructor(key: string) {
    super(`Key not found: ${key}`)
    this.name = 'NotFoundError'
  }
}

export class ValidationError extends MiniDbError {
  constructor(message: string) {
    super(message)
    this.name = 'ValidationError'
  }
}

export class SizeLimitError extends ValidationError {
  constructor(size: number, limit: number) {
    super(`Payload size ${size} exceeds limit ${limit}`)
    this.name = 'SizeLimitError'
  }
}

export class CorruptionError extends MiniDbError {
  constructor(message: string, public readonly key?: string) {
    super(message)
    this.name = 'CorruptionError'
  }
}
