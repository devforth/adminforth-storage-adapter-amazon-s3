export interface AdapterOptions {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  s3ACL?: string,

  /**
   * Sign the Content-Type of uploads, so the client can't store a file with another type than the one
   * the upload plugin picked by the file extension (e.g. text/html under a .png key).
   * Turn it on after updating @adminforth/bulk-ai-flow, @adminforth/markdown, @adminforth/rich-editor and
   * @adminforth/oauth: their older versions send the browser type and get 403 when it differs.
   * Default: false. It will become true in the next major version.
   */
  strictContentType?: boolean;
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