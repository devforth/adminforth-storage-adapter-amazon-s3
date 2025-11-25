import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  PutObjectTaggingCommand,
  HeadBucketCommand,
  ObjectCannedACL,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Readable } from 'stream';

import type { StorageAdapter } from "adminforth";
import type { AdapterOptions } from "./types.js";

const CLEANUP_TAG_KEY = "adminforth-candidate-for-cleanup";
const CLEANUP_RULE_ID = "adminforth-unused-cleaner";

export default class AdminForthAdapterS3Storage implements StorageAdapter {
  private s3: S3Client;
  private options: AdapterOptions;

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
      return `https://${this.options.bucket}/${key}`;
    }
    // If the bucket is private, generate a presigned URL
    // that expires in the specified time
    // (default is 1 hour)
    return await getSignedUrl(this.s3, command, { expiresIn });
  }

  async markKeyForDeletion(key: string): Promise<void> {
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

    let ruleExists = false;
    try {
      const res = await this.s3.send(
        new GetBucketLifecycleConfigurationCommand({ Bucket: this.options.bucket })
      );
      ruleExists = res.Rules?.some((r) => r.ID === CLEANUP_RULE_ID) ?? false;
    } catch (e: any) {
      if (e.name !== "NoSuchLifecycleConfiguration") {
        console.error(`Error checking lifecycle config:`, e);
        throw e;
      }
    }

    if (!ruleExists) {
      await this.s3.send(
        new PutBucketLifecycleConfigurationCommand({
          Bucket: this.options.bucket,
          LifecycleConfiguration: {
            Rules: [
              {
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
              },
            ],
          },
        })
      );
      console.log(`✅ Lifecycle rule "${CLEANUP_RULE_ID}" created.`);
    } else {
      console.log(`ℹ️ Lifecycle rule "${CLEANUP_RULE_ID}" already exists.`);
    }
  }

  objectCanBeAccesedPublicly(): Promise<boolean> {
    return Promise.resolve(this.options.s3ACL === "public-read");
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
}
