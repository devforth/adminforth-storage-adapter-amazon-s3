import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  PutObjectTaggingCommand,
  HeadBucketCommand,
  ObjectCannedACL,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
  LifecycleRule,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CompletedPart,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Readable } from 'stream';

import type { StorageAdapter } from "adminforth";
import { afLogger } from "adminforth";
import type { AdapterOptions, ObjectWriter } from "./types.js";

const CLEANUP_TAG_KEY = "adminforth-candidate-for-cleanup";
const CLEANUP_RULE_ID = "adminforth-unused-cleaner";
const INCOMPLETE_UPLOADS_RULE_ID = "adminforth-incomplete-uploads-cleaner";

export default class AdminForthAdapterS3Storage implements StorageAdapter {
  protected s3: S3Client;
  protected options: AdapterOptions;

  // Shared across all adapter instances in this process. Many instances
  // (one per resource/column) call setupLifecycle concurrently against the
  // same bucket; a bare Set would still race (check-then-act), so we cache
  // the in-flight promise per bucket and let concurrent callers await it.
  // This also prevents concurrent PutBucketLifecycleConfiguration calls that
  // S3 rejects with "OperationAborted" (409).
  private static lifecycleSetupByBucket: Map<string, Promise<void>> = new Map();

  constructor(options: AdapterOptions) {
    this.options = options;
  }

  async getUploadSignedUrl(key: string, contentType: string, expiresIn = 3600): Promise<{ uploadUrl: string, uploadExtraParams:  Record<string, string> }> {
    const tagline = `${CLEANUP_TAG_KEY}=true`;
    const command = new PutObjectCommand({
      Bucket: this.options.bucket,
      ContentType: contentType,
      ACL: (this.options.s3ACL || 'private') as  ObjectCannedACL,
      Key: key,
      Tagging: tagline,
    });
    const uploadUrl = await getSignedUrl(this.s3, command, { expiresIn, unhoistableHeaders: new Set(['x-amz-tagging']) });
    return {
      uploadUrl,
      uploadExtraParams: {
        'x-amz-tagging': tagline
      }
    };
  }

  async getDownloadUrl(key: string, expiresIn = 3600): Promise<string> {
    const command = new GetObjectCommand({
      Bucket: this.options.bucket,
      Key: key,
    });
    if (this.options.s3ACL === "public-read") {
      return `https://${this.options.bucket}.s3.${this.options.region}.amazonaws.com/${key}`;
    }
    // If the bucket is private, generate a presigned URL
    // that expires in the specified time
    // (default is 1 hour)
    return await getSignedUrl(this.s3, command, { expiresIn });
  }

  async markKeyForDeletation(key: string): Promise<void> {
    afLogger.error("Method \"markKeyForDeletation\" is deprecated, use markKeyForDeletion instead");
    await this.markKeyForDeletion(key);
  }

  async markKeyForNotDeletation(key: string): Promise<void> {
    afLogger.error("Method \"markKeyForNotDeletation\" is deprecated, use markKeyForNotDeletion instead");
    await this.markKeyForNotDeletion(key);
  }

  async markKeyForDeletion(key: string): Promise<void> {
    afLogger.debug(`Marking key "${key}" for deletion in bucket "${this.options.bucket}"`);
    const command = new PutObjectTaggingCommand({
      Bucket: this.options.bucket,
      Key: key,
      Tagging: {
        TagSet: [{ Key: CLEANUP_TAG_KEY, Value: "true" }],
      },
    });
    await this.s3.send(command);
  }

  async markKeyForNotDeletion(key: string): Promise<void> {
    afLogger.debug(`Unmarking key "${key}" for deletion in bucket "${this.options.bucket}"`);
    const command = new PutObjectTaggingCommand({
      Bucket: this.options.bucket,
      Key: key,
      Tagging: {
        TagSet: [],
      },
    });
    await this.s3.send(command);
  }

  async setupLifecycle(): Promise<void> {
    if (!this.options.accessKeyId || !this.options.secretAccessKey) {
      throw new Error("Missing AWS credentials in environment variables");
    }
    this.s3 = new S3Client({
      region: this.options.region,
      credentials: {
        accessKeyId: this.options.accessKeyId,
        secretAccessKey: this.options.secretAccessKey,
      },
    });
    try {
      await this.s3.send(new HeadBucketCommand({ Bucket: this.options.bucket }));
    } catch {
      throw new Error(`Bucket "${this.options.bucket}" does not exist`);
    }

    const bucket = this.options.bucket;
    let setupPromise = AdminForthAdapterS3Storage.lifecycleSetupByBucket.get(bucket);
    if (!setupPromise) {
      setupPromise = this.ensureCleanupLifecycleRule();
      AdminForthAdapterS3Storage.lifecycleSetupByBucket.set(bucket, setupPromise);
      // On failure, drop the cached promise so a later call can retry instead
      // of permanently reusing a rejected result.
      setupPromise.catch(() => {
        AdminForthAdapterS3Storage.lifecycleSetupByBucket.delete(bucket);
      });
    }
    await setupPromise;
  }

  private async ensureCleanupLifecycleRule(): Promise<void> {
    let existingRules: LifecycleRule[] = [];
    try {
      const res = await this.s3.send(
        new GetBucketLifecycleConfigurationCommand({ Bucket: this.options.bucket })
      );
      existingRules = res.Rules ?? [];
    } catch (e: any) {
      if (e.name !== "NoSuchLifecycleConfiguration") {
        afLogger.error(`Error checking lifecycle config: ${e}`);
        throw e;
      }
    }

    const cleanupRule: LifecycleRule = {
      ID: CLEANUP_RULE_ID,
      Status: "Enabled",
      Filter: {
        Tag: {
          Key: CLEANUP_TAG_KEY,
          Value: "true",
        },
      },
      Expiration: {
        Days: 2,
      },
    };

    const incompleteUploadsRule: LifecycleRule = {
      ID: INCOMPLETE_UPLOADS_RULE_ID,
      Status: "Enabled",
      Filter: { Prefix: "" },
      AbortIncompleteMultipartUpload: {
        DaysAfterInitiation: 1,
      },
    };

    const missingRules = [cleanupRule, incompleteUploadsRule].filter(
      (rule) => !existingRules.some((r) => r.ID === rule.ID)
    );
    if (!missingRules.length) {
      afLogger.info(`ℹ️ Lifecycle rules "${CLEANUP_RULE_ID}", "${INCOMPLETE_UPLOADS_RULE_ID}" already exist.`);
      return;
    }

    await this.s3.send(
      new PutBucketLifecycleConfigurationCommand({
        Bucket: this.options.bucket,
        LifecycleConfiguration: {
          // Preserve any existing rules; PutBucketLifecycleConfiguration
          // replaces the whole configuration, so we merge instead of overwrite.
          Rules: [...existingRules, ...missingRules],
        },
      })
    );
    afLogger.info(`✅ Lifecycle rule(s) ${missingRules.map((r) => `"${r.ID}"`).join(", ")} created.`);
  }

  objectCanBeAccesedPublicly(): Promise<boolean> {
    return Promise.resolve(this.options.s3ACL === "public-read");
  }

  async isInternalUrl(url: string): Promise<boolean> {
    try {
      const parsedUrl = new URL(url);
      const standardHost = `${this.options.bucket}.s3.${this.options.region}.amazonaws.com`;
      const legacyHost = `${this.options.bucket}.s3.amazonaws.com`;
      
      if (parsedUrl.hostname === standardHost || parsedUrl.hostname === legacyHost) {
        return true;
      }

      if (parsedUrl.hostname.includes('amazonaws.com') && parsedUrl.pathname.startsWith(`/${this.options.bucket}/`)) {
        return true;
      }
      return false;
    } catch (e) {
      return false;
    }
  }
  /**
   * This method should return the key as a data URL (base64 encoded string).
   * @param key - The key of the file to be converted to a data URL
   * @returns A promise that resolves to a string containing the data URL
   */
  async getKeyAsDataURL(key: string): Promise<string> {
    const command = new GetObjectCommand({
      Bucket: this.options.bucket,
      Key: key,
    });

    const body = await this.s3.send(command);
    const stream = body.Body;

    if (!(stream instanceof Readable)) {
      throw new Error("Expected Body to be a Readable stream");
    }

    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }

    const buffer = Buffer.concat(chunks);
    const base64String = buffer.toString('base64');
    const contentType = body.ContentType || 'application/octet-stream';

    return `data:${contentType};base64,${base64String}`;
  }

  /**
   * Starts an S3 multipart upload  returns a writer which streams the data into it,
   * so arbitrarily large files can be written without keeping them in memory.
   * The object appears in the bucket only after close(); abort() discards the uploaded parts.
   * @param key - The keandy of the file to be written e.g. "uploads/file.txt"
   * @param contentType - The MIME type of the file to be written e.g. "image/png"
   */
  async createWriteStream(key: string, contentType: string, bufferSizeMb: number = 5): Promise<ObjectWriter> {
    const bufferSize = bufferSizeMb * 1024 * 1024;
    const bucket = this.options.bucket;
    const { UploadId: uploadId } = await this.s3.send(
      new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        ContentType: contentType,
        ACL: (this.options.s3ACL || 'private') as ObjectCannedACL,
        Tagging: `${CLEANUP_TAG_KEY}=true`,
      })
    );
    if (!uploadId) {
      throw new Error(`S3 did not return UploadId for multipart upload of "${key}"`);
    }
    afLogger.debug(`Started multipart upload "${uploadId}" for key "${key}" in bucket "${bucket}"`);


    const parts: CompletedPart[] = [];
    let pending: Buffer[] = [];
    let pendingLength = 0;
    let partNumber = 0;
    let finished = false;

    let queue: Promise<unknown> = Promise.resolve();

    const enqueue = <T>(fn: () => Promise<T>): Promise<T> => {
      const result = queue.then(fn);
      queue = result.catch(() => {});
      return result;
    };

    const abortUpload = async (): Promise<void> => {
      await this.s3.send(
        new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId })
      );
    };

    const abortAfterFailure = async (e: unknown): Promise<never> => {
      finished = true;
      try {
        await abortUpload();
      } catch (abortError) {
        afLogger.error(`Could not abort multipart upload "${uploadId}" for key "${key}": ${abortError}`);
      }
      throw e;
    };

    const uploadPart = async (body: Buffer): Promise<void> => {
      partNumber += 1;
      const currentPartNumber = partNumber;
      const res = await this.s3.send(
        new UploadPartCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
          PartNumber: currentPartNumber,
          Body: body,
        })
      );
      parts.push({ PartNumber: currentPartNumber, ETag: res.ETag });
    };

    const flushPending = async (): Promise<void> => {
      const body = Buffer.concat(pending, pendingLength);
      pending = [];
      pendingLength = 0;
      await uploadPart(body);
    };

    return {
      write: (data: string | Buffer | Uint8Array): Promise<void> => enqueue(async () => {
        if (finished) {
          throw new Error(`Can't write to "${key}": the writer is already closed or aborted`);
        }
        const chunk = typeof data === 'string'
          ? Buffer.from(data)
          : Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (chunk.length === 0) {
          return;
        }
        pending.push(chunk);
        pendingLength += chunk.length;
        if (pendingLength >= bufferSize) {
          await flushPending().catch(abortAfterFailure);
        }
      }),

      close: (): Promise<void> => enqueue(async () => {
        if (finished) {
          throw new Error(`Can't close "${key}": the writer is already closed or aborted`);
        }
        finished = true;
        try {
          if (pendingLength > 0 || parts.length === 0) {
            await flushPending();
          }
          await this.s3.send(
            new CompleteMultipartUploadCommand({
              Bucket: bucket,
              Key: key,
              UploadId: uploadId,
              MultipartUpload: {
                Parts: [...parts].sort((a, b) => a.PartNumber! - b.PartNumber!),
              },
            })
          );
        } catch (e) {
          return abortAfterFailure(e);
        }
        afLogger.debug(`Completed multipart upload "${uploadId}" for key "${key}" with ${parts.length} part(s)`);
      }),

      abort: (): Promise<void> => enqueue(async () => {
        if (finished) {
          return;
        }
        finished = true;
        pending = [];
        pendingLength = 0;
        await abortUpload();
        afLogger.debug(`Aborted multipart upload "${uploadId}" for key "${key}"`);
      }),
    };
  }
}
