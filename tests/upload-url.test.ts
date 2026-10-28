import { beforeEach, describe, expect, it, vi } from 'vitest';
import { S3Client } from '@aws-sdk/client-s3';

const afLogger = vi.hoisted(() => ({ warn: vi.fn(), debug: vi.fn(), error: vi.fn(), info: vi.fn() }));
vi.mock('adminforth', () => ({ afLogger }));

const OPTIONS = { bucket: 'test-bucket', region: 'eu-central-1', accessKeyId: 'AKIATEST', secretAccessKey: 'secret' };

// signing works offline, so the tests read the signed headers from the URL without calling S3
async function signedHeaders(options: Record<string, unknown>, contentLength?: number) {
  const Adapter = (await import('../index.js')).default;
  const adapter: any = new Adapter({ ...OPTIONS, ...options } as any);
  adapter.s3 = new S3Client({ region: OPTIONS.region, credentials: { accessKeyId: OPTIONS.accessKeyId, secretAccessKey: OPTIONS.secretAccessKey } });
  const { uploadUrl } = await adapter.getUploadSignedUrl('photos/cat.png', 'image/png', 600, contentLength);
  return new URL(uploadUrl).searchParams.get('X-Amz-SignedHeaders')!.split(';');
}

beforeEach(() => {
  vi.resetModules();
  afLogger.warn.mockClear();
});

describe('getUploadSignedUrl', () => {
  it('keeps the URL as before by default: content type and size are not signed', async () => {
    expect(await signedHeaders({})).toEqual(['host', 'x-amz-tagging']);
  });

  it('signs the content type with strictContentType', async () => {
    expect(await signedHeaders({ strictContentType: true })).toContain('content-type');
  });

  it('signs the size when it is passed', async () => {
    expect(await signedHeaders({}, 300)).toContain('content-length');
  });
});

describe('strictContentType warning', () => {
  it('warns once per process when the option is off', async () => {
    const Adapter = (await import('../index.js')).default;
    // without credentials setupLifecycle stops right after the warning, before any S3 call
    const withoutCredentials = { ...OPTIONS, accessKeyId: '' };

    await expect(new Adapter(withoutCredentials as any).setupLifecycle()).rejects.toThrow();
    await expect(new Adapter(withoutCredentials as any).setupLifecycle()).rejects.toThrow();

    expect(afLogger.warn).toHaveBeenCalledTimes(1);
    expect(afLogger.warn.mock.calls[0][0]).toContain('strictContentType is off');
  });

  it('does not warn when the option is on', async () => {
    const Adapter = (await import('../index.js')).default;

    await expect(new Adapter({ ...OPTIONS, accessKeyId: '', strictContentType: true } as any).setupLifecycle()).rejects.toThrow();

    expect(afLogger.warn).not.toHaveBeenCalled();
  });
});
