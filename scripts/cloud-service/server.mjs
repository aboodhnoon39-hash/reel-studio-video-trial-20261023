import http from 'node:http';import fs from 'node:fs';import {createHash,timingSafeEqual} from 'node:crypto';import {fileURLToPath} from 'node:url';import path from 'node:path';import {createCloudMediaPipeline,CloudMediaError} from '../dev-app/cloud-media-pipeline.mjs';
const json=(res,status,value)=>{res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-content-type-options':'nosniff','referrer-policy':'no-referrer'});res.end(JSON.stringify(value));};
async function body(req){let size=0,parts=[];for await(const chunk of req){size+=chunk.length;if(size>16384)throw new CloudMediaError('body_too_large','Request too large',413);parts.push(chunk);}try{return JSON.parse(Buffer.concat(parts).toString('utf8'));}catch{throw new CloudMediaError('invalid_json','Invalid JSON',400);}}
export function createCloudGateway({adminToken,storageRoot,ffmpegPath,ffprobePath,maxUploadBytes=32*1024*1024}={}){
 if(typeof adminToken!=='string'||adminToken.length<32)throw Error('A private admin token of at least 32 characters is required');
 const digest=value=>createHash('sha256').update(value).digest(),adminHash=digest(adminToken),engine=createCloudMediaPipeline({storageRoot,ffmpegPath,ffprobePath,maxUploadBytes});
 const server=http.createServer(async(req,res)=>{try{
  const url=new URL(req.url,'http://cloud.invalid');
  if(req.headers.origin)return json(res,403,{error:'cross_origin_browser_access_disabled'});
  if(req.method==='GET'&&url.pathname==='/health')return json(res,200,{status:'ok',scope:'deployment-candidate',maxProcessingSeconds:60,persistentStorage:false});
  if(req.method==='POST'&&url.pathname==='/api/cloud/tenants'){
   const supplied=String(req.headers.authorization??'');if(!supplied.startsWith('Bearer ')||!timingSafeEqual(digest(supplied.slice(7)),adminHash))return json(res,401,{error:'unauthorized'});
   const d=await body(req);return json(res,201,await engine.registerTenant({tenantId:d.tenantId,label:d.label}));
  }
  const access={tenantId:req.headers['x-tenant-id'],token:req.headers['x-tenant-token']};await engine.authenticateTenant(access.tenantId,access.token);
  if(req.method==='POST'&&url.pathname==='/api/cloud/upload')return json(res,201,await engine.uploadObject({...access,fileName:url.searchParams.get('name')??'upload.mp4',streamOrBuffer:req}));
  if(req.method==='GET'&&url.pathname==='/api/cloud/objects')return json(res,200,{objects:await engine.listObjects(access)});
  if(req.method==='POST'&&url.pathname==='/api/cloud/jobs'){const d=await body(req);return json(res,201,await engine.createAndRunJob({...access,sourceObjectId:d.sourceObjectId,operations:d.operations??{}}));}
  if(req.method==='GET'&&url.pathname==='/api/cloud/jobs')return json(res,200,await engine.getJob({...access,jobId:url.searchParams.get('jobId')}));
  if(req.method==='GET'&&url.pathname==='/api/cloud/download'){
   const out=await engine.resolveOutputVideoFile({...access,jobId:url.searchParams.get('jobId')});let start=0,end=out.size-1,status=200;
   if(req.headers.range){const m=/^bytes=(\d+)-(\d*)$/.exec(req.headers.range);if(!m)return json(res,416,{error:'invalid_range'});start=Number(m[1]);end=m[2]?Number(m[2]):end;if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start>end||end>=out.size)return json(res,416,{error:'invalid_range'});status=206;}
   res.writeHead(status,{'content-type':'video/mp4','cache-control':'no-store','accept-ranges':'bytes','content-length':end-start+1,'x-content-type-options':'nosniff',...(status===206?{'content-range':`bytes ${start}-${end}/${out.size}`}:{})});fs.createReadStream(out.file,{start,end}).on('error',()=>res.destroy()).pipe(res);return;
  }
  return json(res,404,{error:'not_found'});
 }catch(error){if(res.headersSent)return res.destroy();json(res,error instanceof CloudMediaError?error.status:500,{error:error instanceof CloudMediaError?error.code:'internal_error'});}});
 server.requestTimeout=150000;server.headersTimeout=15000;return server;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const port=Number(process.env.PORT??7860);if(!Number.isInteger(port)||port<1024||port>65535)throw Error('Invalid port');
 const server=createCloudGateway({adminToken:process.env.CLOUD_ADMIN_TOKEN,storageRoot:process.env.CLOUD_STORAGE_ROOT??'/data',ffmpegPath:process.env.FFMPEG_PATH??'/usr/bin/ffmpeg',ffprobePath:process.env.FFPROBE_PATH??'/usr/bin/ffprobe'});
 server.listen(port,process.env.HOST??'127.0.0.1',()=>console.log('Private video worker listening'));
}
