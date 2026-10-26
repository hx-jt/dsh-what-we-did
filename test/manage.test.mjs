import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,readFileSync,writeFileSync,rmSync,readdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { join,resolve } from 'node:path'
import { execFileSync,spawnSync } from 'node:child_process'
const script=fileURLToPath(new URL('../scripts/manage.mjs',import.meta.url))
const old={id:'S:1',sessionId:'S',turn:1,summary:'原始请求',reply:'建议，不证明已执行',taskIds:['T'],startedAt:1,endedAt:2,updatedAt:3,status:'replied',sourceSeqs:[1,2,3],requestSeqs:[2]}
test('offline migration backs up before writing, idempotency and rollback/export leave originals intact',()=>{
 const dir=mkdtempSync(join(tmpdir(),'wwd-migrate-')),source=join(dir,'task_ui_did.json'),dest=join(dir,'what_we_did.json'),backups=join(dir,'backup')
 try{
  const bytes=JSON.stringify({unit:{name:'task_ui_did',version:1},global:null,tables:{records:{[old.id]:old}}})
  writeFileSync(source,bytes)
  const args=['migrate','--source',source,'--destination',dest,'--backup-dir',backups,'--offline','--legacy-disabled']
  assert.notEqual(spawnSync(process.execPath,[script,...args.filter(x=>x!=='--legacy-disabled')]).status,0)
  const result=JSON.parse(execFileSync(process.execPath,[script,...args],{encoding:'utf8'}))
  assert.equal(result.imported,1)
  assert.equal(JSON.parse(execFileSync(process.execPath,[script,...args],{encoding:'utf8'})).imported,0)
  const envelope=JSON.parse(readFileSync(dest,'utf8')),state=envelope.tables.state.ledger
  assert.deepEqual(state.records['S:1'].legacy.original,old)
  assert.equal(state.events.length,1)
  assert.equal(state.records['S:1'].updatedAt,3)
  assert.equal(readFileSync(source,'utf8'),bytes)
  const exported=join(dir,'export.json')
  execFileSync(process.execPath,[script,'export','--destination',dest,'--output',exported,'--offline'])
  assert.equal(JSON.parse(readFileSync(exported,'utf8')).records.length,1)
  assert.ok(readdirSync(backups).length>=2)
  const manifest=JSON.parse(readFileSync(result.manifest,'utf8'))
  assert.ok(manifest.files.filter(f=>!f.absent).every(f=>f.sha256.length===64))
  const restored=join(dir,'restored.json')
  execFileSync(process.execPath,[script,'restore','--manifest',result.manifest,'--destination',restored,'--offline','--new-plugin-disabled','--execute','--entry','legacy'])
  assert.equal(JSON.parse(readFileSync(restored,'utf8')).unit.name,'task_ui_did')
 }finally{rmSync(dir,{recursive:true,force:true})}
})
test('offline config pending confirmation, archive maintenance and config rollback retain audits',()=>{
 const dir=mkdtempSync(join(tmpdir(),'wwd-policy-')),source=join(dir,'task_ui_did.json'),dest=join(dir,'what_we_did.json'),backup=join(dir,'backup'),patch=join(dir,'patch.json')
 const run=(command,...extra)=>JSON.parse(execFileSync(process.execPath,[script,command,'--destination',dest,'--offline','--backup-dir',backup,...extra],{encoding:'utf8'}))
 try{
  writeFileSync(source,JSON.stringify({unit:{name:'task_ui_did',version:1},tables:{records:{[old.id]:old}}}))
  run('migrate','--source',source,'--legacy-disabled')
  writeFileSync(patch,JSON.stringify({retention:{mode:'ttl',durationMs:86400000,expiryAction:'archive'}}))
  const pending=run('config','--patch',patch,'--version','1','--reason','explicit archive policy')
  assert.equal(pending.pendingConfirmation,true)
  run('confirm-policy','--version','1','--confirm-retention')
  const preview=run('maintain');assert.deepEqual(preview.recordIds,['S:1'])
  run('maintain','--execute','--authorize-maintenance')
  let state=JSON.parse(readFileSync(dest,'utf8')).tables.state.ledger
  assert.equal(Object.keys(state.records).length,0);assert.ok(state.archives['S:1'])
  run('rollback-config','--version','2','--reason','restore forever')
  state=JSON.parse(readFileSync(dest,'utf8')).tables.state.ledger
  assert.equal(state.config.retention.mode,'forever');assert.equal(state.configAudit.length,2)
  assert.equal(Object.keys(state.records).length,0,'config rollback is not data restoration')
 }finally{rmSync(dir,{recursive:true,force:true})}
})
test('reply repair CLI is dry by default, backups original bytes, corrects once and restores exactly',async()=>{
 const {fixture,damageReply}=await import('./fixtures.mjs')
 const {ledger:l}=await fixture();await damageReply(l)
 // Exercise loading a real 0.1.0 state without ingestion metadata.
 const state=structuredClone(l.state);delete state.ingestion
 const dir=mkdtempSync(join(tmpdir(),'wwd-repair-')),dest=join(dir,'what_we_did.json'),backup=join(dir,'backup')
 try{
  const bytes=JSON.stringify({unit:{name:'what_we_did',version:1},global:null,tables:{state:{ledger:state}}})
  writeFileSync(dest,bytes)
  const args=['repair-replies','--destination',dest,'--backup-dir',backup,'--offline']
  const dry=JSON.parse(execFileSync(process.execPath,[script,...args],{encoding:'utf8'}))
  assert.equal(dry.repairableCount,1);assert.equal(dry.repairedCount,0)
  assert.equal(readFileSync(dest,'utf8'),bytes,'dry-run must not write or silently upgrade')
  assert.equal(dry.records[0].oldLength,0);assert.ok(dry.records[0].newLength>0)
  assert.ok(!JSON.stringify(dry).includes('last valid reply'),'do not dump user text')
  const result=JSON.parse(execFileSync(process.execPath,[script,...args,'--execute','--reason','repair 0.1.0 replay corruption'],{encoding:'utf8'}))
  assert.equal(result.repairedCount,1)
  const repaired=JSON.parse(readFileSync(dest,'utf8')).tables.state.ledger
  assert.equal(repaired.records['S:1'].replyExcerpt,'last valid reply')
  assert.equal(repaired.records['S:1'].updatedAt,state.records['S:1'].updatedAt)
  assert.deepEqual(repaired.events.slice(0,state.events.length),state.events)
  const first=readFileSync(dest,'utf8')
  const repeated=JSON.parse(execFileSync(process.execPath,[script,...args,'--execute','--reason','repeat'],{encoding:'utf8'}))
  assert.equal(repeated.repairedCount,0);assert.equal(readFileSync(dest,'utf8'),first)
  const restored=join(dir,'restored.json')
  execFileSync(process.execPath,[script,'restore','--manifest',result.manifest,'--entry','state','--destination',restored,'--offline','--new-plugin-disabled','--execute'])
  assert.equal(readFileSync(restored,'utf8'),bytes)
 }finally{rmSync(dir,{recursive:true,force:true})}
})
