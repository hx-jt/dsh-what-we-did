import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync,readFileSync,rmSync,writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { boot,send,host,dep,Llm } from './host.mjs'
import { evidencePath } from './evidence.mjs'
const Plugin=await import(process.env.WWD_TEST_PLUGIN_MODULE?pathToFileURL(process.env.WWD_TEST_PLUGIN_MODULE):new URL('../src/index.js',import.meta.url))
async function install(ctx,home){
 for(const module of ['storage/storage','storage/storage-json','storage/storage-domain'])await ctx.plugin((await host(module)).default??await host(module),module.endsWith('storage-json')?{root:home}:module.endsWith('storage-domain')?{backend:'json'}:{})
 return ctx.plugin(Plugin,{})
}
async function detail(ctx,id){return JSON.parse(await ctx.tools.get('did_get').execute({id},{})).record}
const rowEvents=(state,id)=>state.events.filter(e=>e.recordId===id)
test('real replay chain: multiple tools/progress/final -> next turn -> reload -> host restore -> compaction',async()=>{
 const home=mkdtempSync(join(tmpdir(),'wwd-replay-chain-'))
 let runtime=await boot(),ctx=runtime.ctx
 try{
  let plugin=await install(ctx,home)
  const {defineTool}=await dep('@deepseek-ai/dsh-tools')
  ctx.tools.register(defineTool({name:'replay_fixture_tool',description:'controlled replay tool',parameters:{},output:{schema:{type:'string'},render:(_a,v)=>[{type:'text',text:v}]},async execute(){return 'fixture tool result'}}))
  const a=await runtime.agent('replay-A'),final='final verified text '.repeat(40)
  runtime.adapter.script=[{tool:'replay_fixture_tool',text:'same progress'},{tool:'replay_fixture_tool',text:'same progress'},{tool:'replay_fixture_tool'},{text:final}]
  await send(a,'perform multi-step fixture')
  assert.equal(runtime.adapter.requests.length,4)
  const start=a.session.snapshotEvents().find(e=>e.type==='turn/start'),id=`${a.session.id}:${start.seq}`
  const original=await detail(ctx,id)
  assert.equal(original.replyExcerpt,final.slice(0,360));assert.equal(original.replyTruncated,true)
  const state=()=>JSON.parse(readFileSync(join(home,'what_we_did.json'),'utf8')).tables.state.ledger
  const before=rowEvents(state(),id)
  const sourceEvents=a.session.snapshotEvents().filter(e=>['turn/start','user/message','assistant/message','turn/end'].includes(e.type))
  await send(a,'next real user input')
  assert.deepEqual(await detail(ctx,id),original,'next pre-step replay cannot overwrite final reply')
  assert.deepEqual(rowEvents(state(),id),before)
  await plugin.dispose();plugin=await ctx.plugin(Plugin,{})
  await send(a,'continue after plugin reload')
  assert.deepEqual(await detail(ctx,id),original)
  const seed=a.session.snapshotEvents()
  await ctx.fiber.dispose();runtime=await boot();ctx=runtime.ctx;await install(ctx,home)
  const handle=await ctx.agents.create({sessionId:'replay-A',seed,agentOptions:{provider:'capture',model:'capture'}})
  await send(handle.agent,'continue after host restore')
  assert.deepEqual(await detail(ctx,id),original)
  const first=handle.agent.session.snapshotEvents().find(e=>e.type==='user/message'&&e.data.source.kind==='user')
  handle.agent.session.append('user/message',Llm.createUserMessage({content:[{type:'text',text:'opaque compact summary'}],source:{kind:'compact-checkpoint',compactionId:'replay-fixture'}}),{surfaceOp:{op:'replace',startSeq:first.seq,endSeq:first.seq},sourceEventSeqs:[first.seq]})
  await send(handle.agent,'continue after compaction')
  assert.deepEqual(await detail(ctx,id),original)
  assert.deepEqual(rowEvents(state(),id),before)
  assert.ok(runtime.adapter.requests.at(-1).messages.some(m=>m.source?.kind==='what-we-did'))
  writeFileSync(evidencePath('replay-runtime.json'),JSON.stringify({recordId:id,replyChars:original.replyExcerpt.length,replyTruncated:original.replyTruncated,businessEventsUnchanged:true,requests:runtime.adapter.requests.map(r=>r.messages)},null,2))
 }finally{await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true})}
})
test('H real recipient sees empty baseline then offline correction delta on next normal request',async()=>{
 const {fixture,damageReply}=await import('./fixtures.mjs')
 const {execFileSync}=await import('node:child_process')
 const {fileURLToPath}=await import('node:url')
 const {ledger:l,tick,now}=await fixture();tick(Date.now()-now());await damageReply(l,'source-A')
 const home=mkdtempSync(join(tmpdir(),'wwd-correction-loop-')),destination=join(home,'what_we_did.json')
 writeFileSync(destination,JSON.stringify({unit:{name:'what_we_did',version:1},global:null,tables:{state:{ledger:l.state}}}))
 let runtime=await boot(),ctx=runtime.ctx
 try{
  await install(ctx,home);const b=await runtime.agent('recipient-B')
  await send(b,'recall source A')
  const before=runtime.adapter.requests.at(-1).messages.filter(m=>m.source?.kind==='what-we-did')
  assert.equal(before.length,1);assert.match(before[0].content[0].text,/"replyExcerpt":""/)
  const seed=b.session.snapshotEvents()
  await ctx.fiber.dispose() // Offline maintenance is never applied to a live domain.
  const result=JSON.parse(execFileSync(process.execPath,[fileURLToPath(new URL('../scripts/manage.mjs',import.meta.url)),'repair-replies','--destination',destination,'--backup-dir',join(home,'backup'),'--offline','--execute','--reason','controlled replay correction'],{encoding:'utf8'}))
  assert.equal(result.repairedCount,1)
  runtime=await boot();ctx=runtime.ctx;await install(ctx,home)
  assert.equal(runtime.adapter.requests.length,0,'repair does not drive a model')
  const handle=await ctx.agents.create({sessionId:'recipient-B',seed,agentOptions:{provider:'capture',model:'capture'}})
  await send(handle.agent,'continue after correction')
  assert.equal(runtime.adapter.requests.length,1)
  const all=runtime.adapter.requests.at(-1).messages.filter(m=>m.source?.kind==='what-we-did')
  assert.equal(all.length,2)
  assert.match(all.at(-1).content[0].text,/what we did delta/)
  assert.match(all.at(-1).content[0].text,/last valid reply/)
  assert.doesNotMatch(all.at(-1).content[0].text,/"requestExcerpt"/)
  assert.equal(all[0].source.deliveryId,before[0].source.deliveryId)
  const count=handle.agent.session.snapshotEvents().filter(e=>e.type==='user/message'&&e.data.source.kind==='what-we-did').length
  await send(handle.agent,'no further global updates')
  assert.equal(handle.agent.session.snapshotEvents().filter(e=>e.type==='user/message'&&e.data.source.kind==='what-we-did').length,count)
  writeFileSync(evidencePath('correction-runtime.json'),JSON.stringify({repairSummary:{repaired:result.repairedCount},requests:runtime.adapter.requests.map(r=>r.messages)},null,2))
 }finally{await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true})}
})
