declare module 'azurite/dist/src/blob/BlobServer' {
  import BlobConfiguration from 'azurite/dist/src/blob/BlobConfiguration'

  export default class BlobServer {
    constructor(configuration?: BlobConfiguration)
    start(): Promise<void>
    close(): Promise<void>
    getHttpServerAddress(): string
    config: BlobConfiguration
  }
}

declare module 'azurite/dist/src/blob/BlobConfiguration' {
  interface PersistenceLocation {
    locationId: string
    locationPath: string
    maxConcurrency: number
  }

  export default class BlobConfiguration {
    constructor(
      host?: string,
      port?: number,
      keepAliveTimeout?: number,
      metadataDBPath?: string,
      extentDBPath?: string,
      persistencePathArray?: PersistenceLocation[],
      enableAccessLog?: boolean,
      accessLogWriteStream?: NodeJS.WritableStream,
      enableDebugLog?: boolean,
      debugLogFilePath?: string,
      loose?: boolean,
      skipApiVersionCheck?: boolean,
      cert?: string,
      key?: string,
      pwd?: string,
      oauth?: string,
      disableProductStyleUrl?: boolean,
      isMemoryPersistence?: boolean
    )
    host: string
    port: number
  }
}
