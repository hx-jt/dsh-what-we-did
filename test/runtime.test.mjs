import { evidencePath } from './evidence.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { boot, send, dep, host, Llm, texts } from './host.mjs'
import * as Plugin from '../src/index.js'
async function install(ctx,home,config={}) {
  for(const module of ['storage/storage','storage/storage-json','storage/storage-domain']) await ctx.plugin((await host(module)).default??await host(module), module.endsWith('storage-json')?{root:home}:module.endsWith('storage-domain')?{backend:'json'}:{})
  return ctx.plugin(Plugin,config)
}
const ownMessages=r=>r.messages.filter(m=>m.source?.kind==='what-we-did')
test('real loop + JSON: automatic recording, independent input, no task recirculation, restart, compaction, residency',async()=>{
  const home=mkdtempSync(join(tmpdir(),'wwd-runtime-'))
  let runtime=await boot(),ctx=runtime.ctx
  try {
    let plugin=await install(ctx,home,{display:{maxChars:800,deltaMaxChars:600,maxResidentChars:1200}})
    ctx.systemPrompt.context({name:'task-ui:snapshot',order:1,text:'UNCHANGED_TASK_SNAPSHOT'})
    const a=await runtime.agent('A'),b=await runtime.agent('B')
    await send(a,'A first human request')
    assert.equal(ownMessages(runtime.adapter.requests[0]).length,0,'own original is not fed back')
    await send(b,'B request')
    assert.equal(ownMessages(runtime.adapter.requests[1]).length,1)
    await send(b,'B next')
    assert.equal(ownMessages(runtime.adapter.requests[2]).length,1,'no new eligible updates => no appended message')
    await send(a,'A second request')
    await send(b,'B catch up')
    const last=runtime.adapter.requests.at(-1)
    assert.equal(last.messages.filter(m=>m.content.some(x=>x.text?.includes('UNCHANGED_TASK_SNAPSHOT'))).length,1)
    assert.ok(ownMessages(last).reduce((n,m)=>n+m.content.filter(x=>x.type==='text').reduce((n,x)=>n+x.text.length,0),0)<=1200)
    const events=b.session.snapshotEvents()
    const stable=ownMessages(last).map(m=>m.source.deliveryId)
    await plugin.dispose()
    plugin=await ctx.plugin(Plugin,{})
    await send(b,'after reload')
    assert.deepEqual(ownMessages(runtime.adapter.requests.at(-1)).map(m=>m.source.deliveryId),stable)
    // Formal compaction replacement: original request removed, no summary coverage assumed.
    const original=b.session.snapshotEvents().find(e=>e.type==='user/message'&&e.data.source.kind==='user')
    b.session.append('user/message',Llm.createUserMessage({content:[{type:'text',text:'short opaque compaction summary'}],source:{kind:'compact-checkpoint',compactionId:'fixture'}}),{surfaceOp:{op:'replace',startSeq:original.seq,endSeq:original.seq},sourceEventSeqs:[original.seq]})
    await send(b,'after compaction')
    assert.ok(ownMessages(runtime.adapter.requests.at(-1)).some(m=>m.content.some(x=>x.text?.includes('recovery'))))
    writeFileSync(evidencePath('plugin-requests.json'),JSON.stringify(runtime.adapter.requests.map(r=>r.messages),null,2))
    const tools=ctx.tools
    const listed=JSON.parse(await tools.get('did_list').execute({},{}))
    assert.ok(listed.records.some(r=>r.sessionId==='A'))
    assert.equal(listed.records.filter(r=>r.sessionId==='A').length,2)
    // New host and actual seed restore persisted delivery state and sources.
    const seed=b.session.snapshotEvents()
    await ctx.fiber.dispose()
    runtime=await boot();ctx=runtime.ctx
    await install(ctx,home)
    const handle=await ctx.agents.create({sessionId:'B',seed,agentOptions:{provider:'capture',model:'capture'}})
    const before=seed.filter(e=>e.type==='user/message'&&e.data.source.kind==='what-we-did').length
    await send(handle.agent,'after host restart')
    const after=handle.agent.session.snapshotEvents().filter(e=>e.type==='user/message'&&e.data.source.kind==='what-we-did').length
    assert.equal(after,before,'same history resume does not reset epoch or resend window')
  } finally { await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true}) }
})
test('pre-admission rejection retries stable pending; model failure preserves committed source',async()=>{
  const home=mkdtempSync(join(tmpdir(),'wwd-retry-'))
  const {ctx,adapter,agent:create}=await boot()
  try {
    // Outer middleware rejects after the plugin prepares its message.
    let reject=false
    ctx.on('agent/pre-step',async(_p,next)=>{const result=await next();return reject?{kind:'reject'}:result})
    await install(ctx,home)
    const a=await create('A'),b=await create('B')
    await send(a,'source')
    reject=true;await send(b,'rejected')
    assert.equal(adapter.requests.length,1)
    const pending=JSON.parse(await ctx.tools.get('did_diagnostics').execute({sessionId:'B'},{})).recipient.pending.deliveryId
    reject=false;adapter.fail=true
    await send(b,'model fails').catch(()=>{})
    assert.equal(ownMessages(adapter.requests.at(-1))[0].source.deliveryId,pending)
    adapter.fail=false;await send(b,'retry')
    writeFileSync(evidencePath('retry-debug.json'),JSON.stringify(adapter.requests.map(r=>r.messages),null,2))
    assert.equal(ownMessages(adapter.requests.at(-1)).length,1)
    assert.equal(ownMessages(adapter.requests.at(-1))[0].source.deliveryId,pending)
  } finally {await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true})}
})
test('longer real multi-session run respects resident bound while preserving humans and tool-free turn count',async()=>{
 const home=mkdtempSync(join(tmpdir(),'wwd-resident-')), {ctx,adapter,agent:create}=await boot()
 try{
  await install(ctx,home,{display:{maxChars:600,deltaMaxChars:600,maxResidentChars:800,maxRecords:1,deltaMaxEvents:1}})
  const a=await create('source'),r=await create('recipient')
  const humanTexts=[]
  await send(r,'empty initial recipient');humanTexts.push('empty initial recipient')
  for(let i=0;i<8;i++){
   await send(a,`source human ${i}`)
   const text=`recipient human ${i}`;humanTexts.push(text);await send(r,text)
   const request=adapter.requests.at(-1)
   const own=ownMessages(request)
   writeFileSync(evidencePath('residency-debug.json'),JSON.stringify({i,messages:request.messages,events:r.session.snapshotEvents()},null,2))
   assert.ok(own.reduce((n,m)=>n+m.content.filter(b=>b.type==='text').reduce((n,b)=>n+b.text.length,0),0)<=800)
   const humans=request.messages.filter(m=>m.role==='user'&&m.source?.kind==='user').flatMap(m=>m.content.filter(b=>b.type==='text').map(b=>b.text))
   assert.deepEqual(humans,humanTexts)
  }
  assert.ok(r.session.snapshotEvents().some(e=>e.type==='developer/message'&&e.data.message.source.kind==='what-we-did'))
  const result=JSON.parse(await ctx.tools.get('did_list').execute({sessionId:'source',limit:100},{}))
  assert.equal(result.records.length,8)
  writeFileSync(evidencePath('residency-requests.json'),JSON.stringify(adapter.requests.map(r=>r.messages),null,2))
 }finally{await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true})}
})
test('real multi-step tool turn stays one did and owned consolidation preserves tool pairs',async()=>{
 const home=mkdtempSync(join(tmpdir(),'wwd-tools-')),{ctx,adapter,agent:create}=await boot()
 try{
  await install(ctx,home,{display:{maxChars:600,deltaMaxChars:600,maxResidentChars:800}})
  const {defineTool}=await dep('@deepseek-ai/dsh-tools')
  ctx.tools.register(defineTool({name:'fixture_tool',description:'controlled integration tool',parameters:{},output:{schema:{type:'string'},render:(_a,v)=>[{type:'text',text:v}]},async execute(){return 'verified tool result'}}))
  const a=await create('A'),b=await create('B')
  adapter.nextTool='fixture_tool';await send(a,'perform fixture tool')
  assert.equal(adapter.requests.length,2)
  const records=JSON.parse(await ctx.tools.get('did_list').execute({sessionId:'A'},{})).records
  assert.equal(records.length,1)
  for(let i=0;i<4;i++){await send(b,`other ${i}`);await send(a,`continue ${i}`)}
  const latest=adapter.requests.at(-1).messages
  assert.ok(latest.some(m=>m.role==='assistant'&&m.content.some(b=>b.type==='tool-call'&&b.id==='capture-tool-call')))
  assert.ok(latest.some(m=>m.content.some(b=>b.type==='text'&&b.text==='verified tool result')))
  assert.ok(a.session.snapshotEvents().some(e=>e.type==='developer/message'&&e.data.message.source.kind==='what-we-did'))
  writeFileSync(evidencePath('tool-pair-request.json'),JSON.stringify(latest,null,2))
 }finally{await ctx.fiber.dispose();rmSync(home,{recursive:true,force:true})}
})
