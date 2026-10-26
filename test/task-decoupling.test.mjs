import { evidencePath } from './evidence.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,rmSync,writeFileSync,readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { boot,host,send } from './host.mjs'
import * as Plugin from '../src/index.js'
import * as Task from '../../dsh-global-task-list-main/lib/index.js'
test('real task-only 0.4.0-demo.4: legacy bytes untouched, no legacy tools/context; task panel survives',async()=>{
 const home=mkdtempSync(join(tmpdir(),'wwd-task-')),old=process.env.DSH_HOME
 process.env.DSH_HOME=home
 const {ctx,adapter,agent:create}=await boot()
 try{
  for(const module of ['storage/storage','storage/storage-json','storage/storage-domain']) await ctx.plugin((await host(module)).default??await host(module),module.endsWith('storage-json')?{root:home}:module.endsWith('storage-domain')?{backend:'json'}:{})
  const legacyFile=join(home,'task_ui_did.json'),legacyBytes='preserved legacy sentinel'
  writeFileSync(legacyFile,legacyBytes)
  const routes=[]
  ctx.provide('webServer',{register(...args){routes.push(args);return()=>{}}})
  ctx.provide('jobs',{events:{subscribe(){return()=>{}}}})
  await ctx.plugin(Task,{openingSnapshot:true,unfinishedOnly:true,promptSection:true,jobStatusMap:{running:'running',stopping:'running',completed:'done',killed:'cancelled',failed:'failed'}})
  assert.equal(ctx.tools.get('did_list'),undefined)
  assert.ok(ctx.tools.get('task_list'));assert.ok(ctx.tools.get('task_update'));assert.ok(routes.length,'task panel API remains registered')
  await ctx.plugin(Plugin,{})
  const a=await create('A'),b=await create('B')
  await send(a,'ordinary request without task calls');await send(b,'recall');await send(a,'another ordinary request');await send(b,'update')
  const last=adapter.requests.at(-1)
  const snapshots=last.messages.filter(m=>m.source?.kind==='task-ui-state')
  assert.equal(snapshots.length,1)
  assert.equal(snapshots[0].source.batch,'baseline')
  assert.doesNotMatch(JSON.stringify(snapshots),/task-ui:did|Recent did ledger/)
  assert.ok(last.messages.some(m=>m.source?.kind==='what-we-did'))
  assert.equal(readFileSync(legacyFile,'utf8'),legacyBytes)
  writeFileSync(evidencePath('task-decoupling.json'),JSON.stringify({routes:routes.length,requests:adapter.requests.map(r=>r.messages)},null,2))
 }finally{await ctx.fiber.dispose();if(old===undefined)delete process.env.DSH_HOME;else process.env.DSH_HOME=old;rmSync(home,{recursive:true,force:true})}
})
