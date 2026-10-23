// Tenant-isolated LOCAL staging pipeline. No hosted storage, cloud deployment or customer identity service is implied.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {randomBytes, randomUUID, createHash, timingSafeEqual} from 'node:crypto';
import {spawn} from 'node:child_process';
import {pipeline} from 'node:stream/promises';
import {Transform, Readable} from 'node:stream';

const TENANT_ID_REGEX = /^user-[a-zA-Z0-9_-]{3,48}$/;
const OBJECT_ID_REGEX = /^obj-[a-f0-9-]{36}$/;
const JOB_ID_REGEX = /^cjob-[a-f0-9-]{36}$/;
const ALLOWED_EXTENSIONS = new Set(['.mp4', '.webm', '.mov', '.m4v']);

export class CloudMediaError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

function sha256Hex(value) {
  return createHash('sha256').update(value).digest('hex');
}

async function fileSha256(filePath) {
  const hash = createHash('sha256');
  for await (const chunk of fs.createReadStream(filePath)) {
    hash.update(chunk);
  }
  return hash.digest('hex');
}

async function atomicWriteJson(filePath, data) {
  await fsp.mkdir(path.dirname(filePath), {recursive: true});
  const tmp = `${filePath}.${randomUUID()}.tmp`;
  await fsp.writeFile(tmp, JSON.stringify(data, null, 2));
  await fsp.rename(tmp, filePath);
}

async function resolveBinary(preferredPath, fallbackName) {
  if (preferredPath) {
    try {
      await fsp.access(preferredPath, fs.constants.X_OK);
      return preferredPath;
    } catch {throw new CloudMediaError('binary_unavailable','أداة الفيديو المحددة غير متاحة.',503);}
  }
  throw new CloudMediaError('binary_unavailable','حدّد أداة فيديو محلية مثبتة داخل المشروع.',503);
}

function runCommand(exe, args, {cwd} = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(exe, args, {cwd, windowsHide: true, timeout:120000, stdio: ['ignore', 'pipe', 'pipe']});
    let out = '';
    let err = '';
    child.stdout.on('data', chunk => { out += chunk; if(out.length>4*1024*1024)child.kill(); });
    child.stderr.on('data', chunk => { err = (err + chunk).slice(-16000); });
    child.on('error', reject);
    child.on('close', code => {
      if (code === 0) resolve(out);
      else reject(new Error(err || out || `Exit ${code}`));
    });
  });
}

export function createCloudMediaPipeline({
  storageRoot,
  protectedOwnerDirs = [],
  ffmpegPath = null,
  ffprobePath = null,
  maxUploadBytes = 256 * 1024 * 1024,
} = {}) {
  if (!storageRoot) throw new Error('storageRoot is required for CloudMediaPipeline');
  const resolvedRoot = path.resolve(storageRoot);
  const tenantsFile = path.join(resolvedRoot, 'tenants-registry.json');
  const protectedPaths = protectedOwnerDirs.map(d => path.resolve(d));
  let registryQueue = Promise.resolve();
  let workerBusy=false,uploadBusy=false;
  async function assertLocalDirectory(directory){
    const relative=path.relative(resolvedRoot,directory);
    if(relative==='..'||relative.startsWith('..'+path.sep)||path.isAbsolute(relative))throw new CloudMediaError('path_traversal_blocked','رفض مسار خارج مساحة التجربة.',403);
    const paths=[resolvedRoot];let current=resolvedRoot;for(const part of relative.split(path.sep).filter(Boolean)){current=path.join(current,part);paths.push(current);}
    for(const item of paths){const st=await fsp.lstat(item);if(st.isSymbolicLink()||!st.isDirectory())throw new CloudMediaError('unsafe_storage_path','رفض رابط أو مسار تخزين غير آمن.',403);}
    const actual=await fsp.realpath(directory),expected=path.resolve(directory);
    if((process.platform==='win32'?actual.toLowerCase():actual)!==(process.platform==='win32'?expected.toLowerCase():expected))throw new CloudMediaError('unsafe_storage_path','رفض مسار تخزين معاد توجيهه.',403);
  }

  function assertSafeTenantId(tenantId) {
    if (typeof tenantId !== 'string' || !TENANT_ID_REGEX.test(tenantId)) {
      throw new CloudMediaError('invalid_tenant_id', 'معرّف المستخدم السحابي غير صالح.', 400);
    }
    return tenantId;
  }

  function tenantRootDir(tenantId) {
    assertSafeTenantId(tenantId);
    const dir = path.resolve(resolvedRoot, 'tenants', tenantId);
    if (!dir.startsWith(path.join(resolvedRoot, 'tenants') + path.sep)) {
      throw new CloudMediaError('path_traversal_blocked', 'رفض مسار خارج نطاق المستأجر.', 403);
    }
    for (const ownerDir of protectedPaths) {
      if (dir.startsWith(ownerDir + path.sep) || dir === ownerDir) {
        throw new CloudMediaError('owner_files_protected', 'ملفات المالك المحلية محمية ولا يمكن للمسار السحابي الكتابة فوقها.', 403);
      }
    }
    return dir;
  }

  async function readRegistry() {
    try {
      const parsed=JSON.parse(await fsp.readFile(tenantsFile, 'utf8'));if(parsed.version!==1||!parsed.tenants||typeof parsed.tenants!=='object'||Array.isArray(parsed.tenants))throw Error('Invalid registry');return parsed;
    } catch(error) {
      if(error.code==='ENOENT')return {version: 1, tenants: {}};
      throw new CloudMediaError('registry_unreadable','تعذر قراءة سجل المستخدمين؛ لم يُستبدل.',500);
    }
  }

  async function registerTenant({tenantId, label = 'Cloud User'} = {}) {
    assertSafeTenantId(tenantId);
    const rawToken = `ctk_${tenantId}_${randomBytes(24).toString('hex')}`;
    const salt = randomBytes(16).toString('hex');
    const tokenHash = sha256Hex(`${salt}:${rawToken}`);
    const createdAt = new Date().toISOString();

    registryQueue = registryQueue.catch(() => {}).then(async () => {
      await fsp.mkdir(resolvedRoot,{recursive:true});await assertLocalDirectory(resolvedRoot);
      const tDir = tenantRootDir(tenantId);
      await fsp.mkdir(tDir,{recursive:true});await assertLocalDirectory(tDir);
      for(const part of ['objects','jobs','outputs']){await fsp.mkdir(path.join(tDir,part),{recursive:true});await assertLocalDirectory(path.join(tDir,part));}
      const reg = await readRegistry();
      if (reg.tenants[tenantId]) {
        throw new CloudMediaError('tenant_already_exists', 'المستخدم مسجل مسبقًا.', 409);
      }
      reg.tenants[tenantId] = {
        tenantId,
        label: String(label).slice(0, 80),
        salt,
        tokenHash,
        createdAt,
      };
      await atomicWriteJson(tenantsFile, reg);
    });
    await registryQueue;
    return {tenantId, token: rawToken, createdAt, isolationScope: `tenants/${tenantId}`};
  }

  async function authenticateTenant(tenantId, token) {
    assertSafeTenantId(tenantId);
    if (typeof token !== 'string' || token.length < 16) {
      throw new CloudMediaError('unauthorized_tenant', 'رمز وصول المستخدم السحابي مفقود أو غير صالح.', 401);
    }
    const reg = await readRegistry();
    const entry = reg.tenants[tenantId];
    if (!entry) {
      throw new CloudMediaError('unknown_tenant', 'المستخدم السحابي غير موجود.', 403);
    }
    await assertLocalDirectory(tenantRootDir(tenantId));
    const actualHash = Buffer.from(sha256Hex(`${entry.salt}:${token}`), 'hex');
    const expectedHash = Buffer.from(entry.tokenHash, 'hex');
    if (actualHash.length !== expectedHash.length || !timingSafeEqual(actualHash, expectedHash)) {
      throw new CloudMediaError('cross_tenant_access_denied', 'رفض الوصول: الرمز لا يطابق مالك الملفات.', 403);
    }
    return entry;
  }

  async function probeMediaFile(filePath) {
    const ffprobe = await resolveBinary(ffprobePath, 'ffprobe');
    const raw = await runCommand(ffprobe, [
      '-v', 'error',
      '-count_frames',
      '-show_streams',
      '-show_format',
      '-of', 'json',
      filePath,
    ]);
    const parsed = JSON.parse(raw);
    const video = (parsed.streams ?? []).find(s => s.codec_type === 'video');
    if (!video) {
      throw new CloudMediaError('invalid_media_container', 'الملف المرفوع لا يحتوي مسار فيديو صالح.', 400);
    }
    const audio = (parsed.streams ?? []).find(s => s.codec_type === 'audio');
    const [fpsNum, fpsDen] = String(video.r_frame_rate ?? '30/1').split('/').map(Number);
    const fps = fpsDen ? Number((fpsNum / fpsDen).toFixed(3)) : 30;
    const duration = Number(parsed.format?.duration ?? video.duration ?? 0);
    const frames = Number(video.nb_read_frames ?? video.nb_frames ?? Math.round(duration * fps));
    if(!Number.isFinite(duration)||duration<=0||duration>1800||!Number.isFinite(fps)||fps<=0||fps>120||!Number.isSafeInteger(frames)||frames<=0||![video.width,video.height].every(n=>Number.isInteger(n)&&n>=2&&n<=7680))throw new CloudMediaError('media_limits','الفيديو خارج حدود التجربة المحلية.',400);
    return {
      width: Number(video.width),
      height: Number(video.height),
      codec: video.codec_name ?? 'unknown',
      fps,
      duration,
      frames,
      hasAudio: Boolean(audio),
      audioCodec: audio?.codec_name ?? null,
    };
  }

  async function uploadObjectInternal({tenantId, token, fileName = 'upload.mp4', streamOrBuffer, sourcePathGuard = null} = {}) {
    await authenticateTenant(tenantId, token);
    if (sourcePathGuard) {
      const resolvedSource = path.resolve(sourcePathGuard);
      for (const ownerDir of protectedPaths) {
        if (resolvedSource.startsWith(ownerDir + path.sep) || resolvedSource === ownerDir) {
          throw new CloudMediaError('owner_project_upload_blocked', 'يُحظر رفع مشاريع أو وسائط المالك الأصلية تلقائيًا إلى المسار السحابي.', 403);
        }
      }
    }
    const ext = path.extname(String(fileName)).toLowerCase() || '.mp4';
    if (!ALLOWED_EXTENSIONS.has(ext)) {
      throw new CloudMediaError('unsupported_media_extension', 'امتداد الفيديو غير مدعوم في التخزين السحابي.', 400);
    }
    const objectId = `obj-${randomUUID()}`;
    const tDir = tenantRootDir(tenantId);
    const binFile = path.join(tDir, 'objects', `${objectId}${ext}`);
    const metaFile = path.join(tDir, 'objects', `${objectId}.json`);
    await fsp.mkdir(path.dirname(binFile), {recursive: true});
    await assertLocalDirectory(path.dirname(binFile));
    const existing=await listObjects({tenantId,token});if(existing.length>=20||existing.reduce((sum,item)=>sum+item.sizeBytes,0)>=512*1024*1024)throw new CloudMediaError('storage_limit','وصلت مساحة تجربة المستخدم إلى حدها.',413);

    let sizeBytes = 0;
    const hash = createHash('sha256');
    const inputStream = Buffer.isBuffer(streamOrBuffer)
      ? Readable.from([streamOrBuffer])
      : streamOrBuffer;

    try {
      await pipeline(
        inputStream,
        new Transform({
          transform(chunk, enc, done) {
            sizeBytes += chunk.length;
            if (sizeBytes > maxUploadBytes) {
              return done(new CloudMediaError('upload_too_large', 'حجم الملف يتجاوز الحد المسموح للرفع السحابي.', 413));
            }
            hash.update(chunk);
            done(null, chunk);
          },
        }),
        fs.createWriteStream(binFile, {flags: 'wx', mode: 0o600})
      );
      if (sizeBytes === 0) {
        throw new CloudMediaError('empty_upload', 'ملف الفيديو المرفوع فارغ.', 400);
      }
      const mediaInfo = await probeMediaFile(binFile);
      const metadata = {
        objectId,
        tenantId,
        originalName: path.basename(String(fileName)).slice(0, 120),
        extension: ext,
        sizeBytes,
        sha256: hash.digest('hex'),
        media: mediaInfo,
        storageKey: `tenants/${tenantId}/objects/${objectId}${ext}`,
        createdAt: new Date().toISOString(),
      };
      await atomicWriteJson(metaFile, metadata);
      return metadata;
    } catch (err) {
      await fsp.unlink(binFile).catch(() => {});
      await fsp.unlink(metaFile).catch(() => {});
      if (err instanceof CloudMediaError) throw err;
      throw new CloudMediaError('upload_validation_failed', 'فشل التحقق من الفيديو المرفوع؛ لم تُعرض تفاصيل المعالجة الداخلية.', 400);
    }
  }

  async function getObjectMetadata({tenantId, token, objectId} = {}) {
    await authenticateTenant(tenantId, token);
    if (!OBJECT_ID_REGEX.test(String(objectId ?? ''))) {
      throw new CloudMediaError('invalid_object_id', 'معرّف الملف السحابي غير صالح.', 400);
    }
    const metaFile = path.join(tenantRootDir(tenantId), 'objects', `${objectId}.json`);
    await assertLocalDirectory(path.dirname(metaFile));
    let meta;
    try {
      meta = JSON.parse(await fsp.readFile(metaFile, 'utf8'));
    } catch {
      throw new CloudMediaError('object_not_found', 'الملف غير موجود ضمن مساحة هذا المستخدم.', 404);
    }
    if (meta.tenantId !== tenantId) {
      throw new CloudMediaError('cross_tenant_access_denied', 'رفض الوصول لملف مستخدم آخر.', 403);
    }
    return meta;
  }

  async function listObjects({tenantId, token} = {}) {
    await authenticateTenant(tenantId, token);
    const objDir = path.join(tenantRootDir(tenantId), 'objects');
    await assertLocalDirectory(objDir);
    await fsp.mkdir(objDir, {recursive: true});
    const entries = await fsp.readdir(objDir);
    const items = [];
    for (const name of entries) {
      if (name.endsWith('.json') && OBJECT_ID_REGEX.test(name.slice(0, -5))) {
        const item = JSON.parse(await fsp.readFile(path.join(objDir, name), 'utf8'));
        if (item.tenantId === tenantId) items.push(item);
      }
    }
    return items.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  async function runJob({
    tenantId,
    token,
    sourceObjectId,
    operations = {},
  } = {}) {
    await authenticateTenant(tenantId, token);
    const sourceMeta = await getObjectMetadata({tenantId, token, objectId: sourceObjectId});
    const tDir = tenantRootDir(tenantId);
    await assertLocalDirectory(path.join(tDir,'objects'));await assertLocalDirectory(path.join(tDir,'jobs'));await assertLocalDirectory(path.join(tDir,'outputs'));
    const sourceFile = path.join(tDir, 'objects', `${sourceMeta.objectId}${sourceMeta.extension}`);
    const actualSourceSha = await fileSha256(sourceFile);
    if (actualSourceSha !== sourceMeta.sha256) {
      throw new CloudMediaError('source_integrity_mismatch', 'بصمة الملف المرفوع لا تطابق السجل المحفوظ.', 409);
    }

    if(!operations||typeof operations!=='object'||Array.isArray(operations)||Object.keys(operations).some(k=>!['trimSeconds','width','height','accentOverlay'].includes(k)))throw new CloudMediaError('invalid_operations','عمليات التجربة غير صالحة.',400);
    const duration=operations.trimSeconds??Math.min(sourceMeta.media.duration,60),width=operations.width??sourceMeta.media.width,height=operations.height??sourceMeta.media.height;
    if(typeof duration!=='number'||!Number.isFinite(duration)||duration<0.1||duration>60||duration>sourceMeta.media.duration||![width,height].every(n=>Number.isInteger(n)&&n>=2&&n<=1920&&n%2===0)||width*height>2073600||operations.accentOverlay!==undefined&&typeof operations.accentOverlay!=='boolean')throw new CloudMediaError('invalid_operations','راجع المدة والأبعاد وحدود المعالجة المحلية.',400);
    const fingerprint=sha256Hex(JSON.stringify({sourceSha256:sourceMeta.sha256,duration,width,height,accentOverlay:operations.accentOverlay===true,version:1}));
    const jobId = `cjob-${fingerprint.slice(0,8)}-${fingerprint.slice(8,12)}-${fingerprint.slice(12,16)}-${fingerprint.slice(16,20)}-${fingerprint.slice(20,32)}`;
    const jobFile = path.join(tDir, 'jobs', `${jobId}.json`);
    try{const existing=JSON.parse(await fsp.readFile(jobFile,'utf8'));if(existing.status==='completed'){const output=await resolveOutputVideoFile({tenantId,token,jobId});if(await fileSha256(output.file)!==existing.output.sha256)throw new CloudMediaError('output_integrity_mismatch','تغير ناتج المهمة؛ لا إعادة تلقائية.',409);return {...existing,reused:true};}throw new CloudMediaError('job_reconciliation_required','المهمة السابقة تحتاج مراجعة؛ لا تكرار معالجة تلقائي.',409);}catch(error){if(error.code!=='ENOENT')throw error;}
    const lockFile=jobFile+'.lock';let lock;try{lock=await fsp.open(lockFile,'wx');}catch{throw new CloudMediaError('job_locked','المهمة محجوزة أو انقطعت سابقًا؛ لا تكرار تلقائي.',409);}
    try{
    const outDir = path.join(tDir, 'outputs', jobId);
    const outFile = path.join(outDir, 'output.mp4');
    const verificationFile = path.join(outDir, 'verification.json');
    await fsp.mkdir(outDir, {recursive: true});

    const maxDuration = Math.min(
      duration,
      Math.max(1, Number(sourceMeta.media.duration || 10))
    );
    const targetWidth = width;
    const targetHeight = height;
    const accentBand = operations.accentOverlay === true;

    const jobRecord = {
      jobId,
      tenantId,
      sourceObjectId: sourceMeta.objectId,
      sourceSha256: sourceMeta.sha256,
      status: 'queued',
      stages: [{stage: 'queued', at: new Date().toISOString()}],
      operations: {
        trimSeconds: maxDuration,
        width: targetWidth,
        height: targetHeight,
        accentOverlay: accentBand,
      },
      output: null,
      verification: null,
      error: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await atomicWriteJson(jobFile, jobRecord);

    try {
      jobRecord.status = 'processing';
      jobRecord.stages.push({stage: 'processing', at: new Date().toISOString()});
      jobRecord.updatedAt = new Date().toISOString();
      await atomicWriteJson(jobFile, jobRecord);

      const ffmpeg = await resolveBinary(ffmpegPath, 'ffmpeg');
      const vfParts = [
        `scale=${targetWidth}:${targetHeight}:force_original_aspect_ratio=decrease`,
        `pad=${targetWidth}:${targetHeight}:(ow-iw)/2:(oh-ih)/2:color=0x10202b`,
      ];
      if (accentBand) {
        vfParts.push(`drawbox=x=0:y=${Math.max(0, targetHeight - 36)}:w=${targetWidth}:h=36:color=0xf0b85c@0.85:t=fill`);
      }

      const args = [
        '-hide_banner',
        '-loglevel', 'error',
        '-i', sourceFile,
        '-map','0:v:0','-map','0:a:0?',
        '-t', String(maxDuration),
        '-vf', vfParts.join(','),
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-crf', '22',
        '-pix_fmt', 'yuv420p',
        ...(sourceMeta.media.hasAudio ? ['-c:a', 'copy'] : ['-an']),
        '-movflags', '+faststart',
        '-y',
        outFile,
      ];
      await runCommand(ffmpeg, args);

      jobRecord.status = 'verifying';
      jobRecord.stages.push({stage: 'verifying', at: new Date().toISOString()});
      jobRecord.updatedAt = new Date().toISOString();
      await atomicWriteJson(jobFile, jobRecord);

      const outStat = await fsp.stat(outFile);
      if (outStat.size <= 0) {
        throw new Error('Rendered output video is empty');
      }
      const outSha256 = await fileSha256(outFile);
      if (outSha256 === sourceMeta.sha256) {
        throw new Error('Output video SHA-256 must differ from raw input after processing');
      }
      const outMedia = await probeMediaFile(outFile);
      if (outMedia.width !== targetWidth || outMedia.height !== targetHeight || outMedia.frames <= 0||Math.abs(outMedia.duration-maxDuration)>Math.max(.15,2/sourceMeta.media.fps)||sourceMeta.media.hasAudio&&!outMedia.hasAudio) {
        throw new Error('Output video dimensions or frame count verification failed');
      }
      await runCommand(ffmpeg,['-v','error','-i',outFile,'-f','null','-']);

      const verification = {
        verified: true,
        deployment:'local-staging',
        cloudHosted:false,
        jobId,
        tenantId,
        sourceObjectId: sourceMeta.objectId,
        sourceSha256: sourceMeta.sha256,
        outputSha256: outSha256,
        videoTransformed: outSha256 !== sourceMeta.sha256,
        sizeBytes: outStat.size,
        width: outMedia.width,
        height: outMedia.height,
        fps: outMedia.fps,
        duration: outMedia.duration,
        frames: outMedia.frames,
        hasAudio: outMedia.hasAudio,
        codec: outMedia.codec,
        verifiedAt: new Date().toISOString(),
      };
      await atomicWriteJson(verificationFile, verification);

      jobRecord.status = 'completed';
      jobRecord.stages.push({stage: 'completed', at: new Date().toISOString()});
      jobRecord.output = {
        storageKey: path.posix.join('tenants', tenantId, 'outputs', jobId, 'output.mp4'),
        downloadPath: `/api/cloud/download?tenantId=${encodeURIComponent(tenantId)}&jobId=${encodeURIComponent(jobId)}`,
        sizeBytes: outStat.size,
        sha256: outSha256,
      };
      jobRecord.verification = verification;
      jobRecord.updatedAt = new Date().toISOString();
      await atomicWriteJson(jobFile, jobRecord);
      return jobRecord;
    } catch (err) {
      jobRecord.status = 'failed';
      jobRecord.error = 'local_processing_failed';
      jobRecord.stages.push({stage: 'failed', at: new Date().toISOString()});
      jobRecord.updatedAt = new Date().toISOString();
      await atomicWriteJson(jobFile, jobRecord);
      throw new CloudMediaError('cloud_job_failed', 'فشلت معالجة التجربة المحلية؛ التفاصيل الداخلية محجوبة.', 500);
    }
    }finally{await lock.close();await fsp.unlink(lockFile).catch(()=>{});}
  }

  async function getJob({tenantId, token, jobId} = {}) {
    await authenticateTenant(tenantId, token);
    if (!JOB_ID_REGEX.test(String(jobId ?? ''))) {
      throw new CloudMediaError('invalid_job_id', 'معرّف المهمة السحابية غير صالح.', 400);
    }
    const jobFile = path.join(tenantRootDir(tenantId), 'jobs', `${jobId}.json`);
    await assertLocalDirectory(path.dirname(jobFile));
    let job;
    try {
      job = JSON.parse(await fsp.readFile(jobFile, 'utf8'));
    } catch {
      throw new CloudMediaError('job_not_found', 'المهمة غير موجودة ضمن مساحة هذا المستخدم.', 404);
    }
    if (job.tenantId !== tenantId) {
      throw new CloudMediaError('cross_tenant_access_denied', 'رفض الوصول لمهمة مستخدم آخر.', 403);
    }
    return job;
  }

  async function listJobs({tenantId, token} = {}) {
    await authenticateTenant(tenantId, token);
    const jobsDir = path.join(tenantRootDir(tenantId), 'jobs');
    await assertLocalDirectory(jobsDir);
    await fsp.mkdir(jobsDir, {recursive: true});
    const entries = await fsp.readdir(jobsDir);
    const items = [];
    for (const name of entries) {
      if (name.endsWith('.json') && JOB_ID_REGEX.test(name.slice(0, -5))) {
        const job = JSON.parse(await fsp.readFile(path.join(jobsDir, name), 'utf8'));
        if (job.tenantId === tenantId) items.push(job);
      }
    }
    return items.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }

  async function resolveOutputVideoFile({tenantId, token, jobId} = {}) {
    const job = await getJob({tenantId, token, jobId});
    if (job.status !== 'completed' || !job.verification?.verified) {
      throw new CloudMediaError('output_not_ready', 'الفيديو الناتج غير جاهز أو لم يجتز التحقق بعد.', 409);
    }
    const tDir = tenantRootDir(tenantId);
    const outFile = path.resolve(tDir, 'outputs', jobId, 'output.mp4');
    await assertLocalDirectory(path.dirname(outFile));
    if (!outFile.startsWith(path.join(tDir, 'outputs') + path.sep)) {
      throw new CloudMediaError('path_traversal_blocked', 'مسار الفيديو الناتج غير آمن.', 403);
    }
    const stat = await fsp.stat(outFile);
    if((await fsp.lstat(outFile)).isSymbolicLink())throw new CloudMediaError('unsafe_storage_path','رفض رابط ناتج غير آمن.',403);
    return {file: outFile, size: stat.size, sha256: job.output.sha256, job};
  }

  return {
    deployment:'local-staging',cloudHosted:false,
    registerTenant,
    authenticateTenant,
    async uploadObject(input){if(uploadBusy)throw new CloudMediaError('upload_busy','رفع آخر قيد التنفيذ.',409);uploadBusy=true;try{return await uploadObjectInternal(input);}finally{uploadBusy=false;}},
    getObjectMetadata,
    listObjects,
    async createAndRunJob(input){if(workerBusy)throw new CloudMediaError('worker_busy','عامل المعالجة مشغول.',409);workerBusy=true;try{return await runJob(input);}finally{workerBusy=false;}},
    getJob,
    listJobs,
    resolveOutputVideoFile,
  };
}
