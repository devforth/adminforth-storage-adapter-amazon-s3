export interface AdapterOptions {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  s3ACL?: string,
}