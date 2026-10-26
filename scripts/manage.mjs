#!/usr/bin/env node
/** Offline JSON-domain maintenance. Reads only explicitly named DID files, never host profiles or credentials. */
import { readFile,writeFile,mkdir,open,rename,unlink } from 'node:fs/promises'
import { resolve,join,dirname } from 'node:path'
import { createHash,randomUUID } from 'node:crypto'
import { Ledger } from '../src/ledger.js'
import { configFor } from '../src/config.js'
const argv=process.argv.slice(2),command=argv.shift(),args={}
for(let i=0;i<argv.length;i++) {
  if(!argv[i].startsWith('--')) throw new Error('expected --option')
  const key=argv[i].slice(2)
  if(Object.hasOwn(args,key)) throw new Error(`duplicate option ${key}`)
  args[key]=argv[i+1]&&!argv[i+1].startsWith('--')?argv[++i]:true
}
const required=key=>{if(typeof args[key]!=='string')throw new Error(`--${key} required`);return resolve(args[key])}
const flag=key=>{if(args[key]!==true)throw new Error(`--${key} required`)}
const hash=bytes=>createHash('sha256').update(bytes).digest('hex')
async function optional(path) {try{return await readFile(path)}catch(e){if(e.code==='ENOENT')return null;throw e}}
async function atomic(path,bytes) {
 await mkdir(dirname(path),{recursive:true,mode:0o700})
 const temporary=`${path}.${randomUUID()}.tmp`
 const fd=await open(temporary,'wx',0o600)
 try{await fd.writeFile(bytes);await fd.sync()}finally{await fd.close()}
 try{await rename(temporary,path)}catch(e){await unlink(temporary).catch(()=>{});throw e}
 const parent=await open(dirname(path),'r');try{await parent.sync()}finally{await parent.close()}
}
function envelope(bytes) {
 const data=JSON.parse(bytes)
 if(data.unit?.name!=='what_we_did'||data.unit.version!==1||!data.tables?.state?.ledger)throw new Error('not a what_we_did JSON domain')
 return data
}
async function backup(files) {
 const root=required('backup-dir'),directory=join(root,`${new Date().toISOString().replaceAll(':','-')}-${randomUUID()}`)
 await mkdir(directory,{recursive:true,mode:0o700})
 const entries=[]
 for(const [kind,path]of Object.entries(files)) {
  const bytes=await optional(path)
  if(!bytes){entries.push({kind,source:path,absent:true});continue}
  const output=join(directory,`${kind}.json`)
  await atomic(output,bytes)
  entries.push({kind,source:path,backup:output,sha256:hash(bytes),bytes:bytes.length})
 }
 const manifest=join(directory,'manifest.json')
 await atomic(manifest,JSON.stringify({format:1,createdAt:new Date().toISOString(),files:entries},null,2))
 return manifest
}
async function main() {
 flag('offline')
 if(command==='restore') {
  flag('new-plugin-disabled');flag('execute')
  if(!['legacy','state'].includes(args.entry))throw new Error('--entry legacy|state required')
  const manifest=JSON.parse(await readFile(required('manifest'),'utf8'))
  const file=manifest.files.find(f=>f.kind===args.entry)
  if(!file||file.absent)throw new Error('backup entry absent; no file is deleted automatically')
  const bytes=await readFile(file.backup)
  if(hash(bytes)!==file.sha256)throw new Error('backup checksum mismatch')
  const destination=required('destination')
  const prior=await optional(destination)
  if(prior) await atomic(`${destination}.before-restore-${randomUUID()}.json`,prior)
  await atomic(destination,bytes)
  return {restored:destination,sha256:file.sha256}
 }
 const destination=required('destination')
 await mkdir(dirname(destination),{recursive:true,mode:0o700})
 const lock=`${destination}.maintenance.lock`
 const fd=await open(lock,'wx',0o600)
 try {
  const before=await optional(destination)
  if(command==='export') {
    if(!before)throw new Error('domain not found')
    const state=envelope(before).tables.state.ledger
    const output=required('output')
    if(output===destination)throw new Error('export cannot overwrite domain')
    await writeFile(output,JSON.stringify({format:1,exportedAt:Date.now(),records:Object.values(state.records),events:state.events,config:state.config,configAudit:state.configAudit,expired:state.expired,archives:state.archives,rollbackNote:'New records are NOT automatically merged back into legacy DID.'},null,2),{flag:'wx',mode:0o600})
    return {exported:output,records:Object.keys(state.records).length}
  }
  if(command==='repair-replies') {
    if(!before) throw new Error('domain not found')
    if(args.execute!==undefined&&args.execute!==true) throw new Error('--execute is a flag')
    const initial=envelope(before).tables.state.ledger
    let current=structuredClone(initial)
    // Dry-run and startup compatibility folding remain entirely in memory.
    const ledger=new Ledger({async load(){return structuredClone(current)},async save(s){current=structuredClone(s)}},{},Date.now,Object.values(initial.records).filter(r=>r.status==='running').map(r=>r.sessionId))
    await ledger.ready
    if(args['record-id']!==undefined&&(typeof args['record-id']!=='string'||!args['record-id'].trim())) throw new Error('--record-id requires a nonempty ID')
    const options={recordIds:args['record-id']!==undefined?[args['record-id']]:undefined}
    if(!args.execute) return {mode:'dry-run',...(await ledger.repairReplies(options))}
    const reason=String(args.reason??'')
    if(!reason.trim()||reason.length>1000) throw new Error('--reason required, maximum 1000 chars')
    const manifest=await backup({state:destination})
    if(hash(await readFile(destination))!==hash(before)) throw new Error('destination changed during offline repair')
    const result=await ledger.repairReplies({...options,execute:true,reason})
    if(result.repairedCount) await atomic(destination,JSON.stringify({unit:{name:'what_we_did',version:1},global:null,tables:{state:{ledger:current}}},null,2))
    return {mode:'execute',...result,manifest}
  }
  if(!['migrate','config','confirm-policy','rollback-config','maintain'].includes(command))throw new Error('commands: migrate, export, config, confirm-policy, rollback-config, maintain, restore, repair-replies')
  if(command!=='migrate'&&!before)throw new Error('domain not found')
  const source=command==='migrate'?required('source'):null
  if(source===destination)throw new Error('source and destination must differ')
  if(source)flag('legacy-disabled')
  let legacyBytes,legacyData
  if(source) {
    legacyBytes=await readFile(source);legacyData=JSON.parse(legacyBytes)
    if(legacyData.unit?.name!=='task_ui_did'||legacyData.unit.version!==1||!legacyData.tables?.records)throw new Error('not a legacy task_ui_did JSON domain')
  }
  const manifest=await backup({...source?{legacy:source}:{},state:destination})
  const initial=before?envelope(before).tables.state.ledger:undefined
  let current=initial
  const ledger=new Ledger({async load(){return current},async save(state){current=structuredClone(state);await atomic(destination,JSON.stringify({unit:{name:'what_we_did',version:1},global:null,tables:{state:{ledger:state}}},null,2))}}, {}, Date.now, Object.values(initial?.records??{}).filter(r=>r.status==='running').map(r=>r.sessionId))
  await ledger.ready
  let result
  if(command==='migrate') {
    if(hash(await readFile(source))!==hash(legacyBytes))throw new Error('legacy source changed during offline migration')
    result=await ledger.importLegacy(Object.values(legacyData.tables.records),{source,sha256:hash(legacyBytes),backupManifest:manifest})
  } else if(command==='config') {
    const patch=JSON.parse(await readFile(required('patch'),'utf8'))
    result=await ledger.updateConfig(patch,String(args.reason??''),Number(args.version),{kind:'user-offline'})
  } else if(command==='confirm-policy') {
    flag('confirm-retention');result=await ledger.confirmPolicy(Number(args.version))
  } else if(command==='rollback-config') {
    const version=Number(args.version),audit=ledger.state.configAudit.findLast(a=>a.configVersion===version)
    if(!audit)throw new Error('config audit version not found')
    result=await ledger.tx(s=>{
      const old=structuredClone(s.config);s.config=configFor(audit.old);s.configVersion++;s.pendingPolicy=null
      const entry={actor:{kind:'user-offline'},reason:`rollback config version ${version}: ${args.reason??'operator request'}`,old,new:structuredClone(s.config),effectiveAt:Date.now(),configVersion:s.configVersion}
      s.configAudit.push(entry);return entry
    })
  } else {
    if(args.execute)flag('authorize-maintenance')
    result=await ledger.maintain({execute:args.execute===true,authorization:args.execute?'user-offline':undefined})
  }
  return {...result,manifest}
 } finally {await fd.close();await unlink(lock)}
}
try{process.stdout.write(JSON.stringify(await main(),null,2)+'\n')}catch(e){process.stderr.write(`what we did: ${e.message}\n`);process.exitCode=1}
