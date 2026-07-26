export interface AdapterOptions {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  s3ACL?: string,
}

/**
 * Mirrors the ObjectWriter shape expected by adminforth's StorageAdapter.createWriteStream,
 * which is not exported from the adminforth package.
 */
export interface ObjectWriter {
  write(data: string | Buffer | Uint8Array): Promise<void>;
  close(): Promise<void>;
  abort(): Promise<void>;
}