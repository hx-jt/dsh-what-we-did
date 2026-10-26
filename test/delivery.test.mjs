import test from 'node:test'
import assert from 'node:assert/strict'
import { fixture, turn, event } from './fixtures.mjs'
import { Delivery } from '../src/delivery.js'
const visible = (messages=[], originals=[], replacement=0) => ({messages,originals,replacement})
async function accept(d,sid,p,v=visible()) {
  const message={deliveryId:p.deliveryId,text:p.text,recordIds:p.recordIds,createdAt:p.createdAt}
  await d.prepare(sid,visible([...v.messages,message],v.originals,v.replacement))
  return message
}
test('independent sessions initialize, no update no message, delta only changed fields', async () => {
  const {ledger:l}=await fixture();const d=new Delivery(l)
  await turn(l,'A')
  const p=await d.prepare('B',visible())
  assert.equal(p.kind,'baseline');assert.match(p.text,/request A/)
  assert.equal(l.state.recipients.B.scanCursor,0,'preparation is not delivery')
  const m=await accept(d,'B',p)
  assert.equal(await d.prepare('B',visible([m])),null)
  await l.note('A:1','new annotation',{sessionId:'C'})
  const delta=await d.prepare('B',visible([m]))
  assert.equal(delta.kind,'delta');assert.match(delta.text,/new annotation/)
  assert.doesNotMatch(delta.text,/request A|reply A/)
  const c=await d.prepare('C',visible())
  assert.equal(c.kind,'baseline')
})
test('pending retries stable; committed source reconciles even after lost ack and restart', async () => {
  const {ledger:l}=await fixture();await turn(l,'A')
  const d=new Delivery(l), p=await d.prepare('B',visible())
  assert.equal((await d.prepare('B',visible())).deliveryId,p.deliveryId)
  const again=new Delivery(l)
  const m=await accept(again,'B',p)
  assert.equal(await again.prepare('B',visible([m])),null)
  assert.equal(l.state.recipients.B.scanCursor,l.state.seq)
})
test('own visible originals excluded, compaction epoch enables bounded own recovery', async () => {
  const {ledger:l}=await fixture();await turn(l,'A');const d=new Delivery(l)
  assert.equal(await d.prepare('A',visible([],[2,3])),null)
  const p=await d.prepare('A',visible([],[],99))
  assert.equal(p.kind,'recovery');assert.match(p.text,/request A/)
  assert.equal(p.epoch,1)
})
test('bounded backlog, same record A start B end A end delivered as patch', async () => {
  const {ledger:l}=await fixture({display:{deltaMaxEvents:1,deltaMaxChars:600,maxChars:600}})
  const d=new Delivery(l)
  assert.equal(await d.prepare('R',visible()),null)
  await l.ingest('A',event(1,'turn/start',{turn:1}))
  let p=await d.prepare('R',visible()),m=await accept(d,'R',p)
  await turn(l,'B')
  await l.ingest('A',event(2,'turn/end',{turn:1,reason:{kind:'completed'}}))
  let seen=[m], count=0
  while ((p=await d.prepare('R',visible(seen)))) {
    assert.ok(p.text.length<=600)
    seen.push(await accept(d,'R',p,visible(seen)))
    assert.ok(++count<12)
  }
  assert.equal(l.state.recipients.R.scanCursor,l.state.seq)
  assert.ok(seen.at(-1).text.includes('endedAt'))
  assert.ok(!seen.at(-1).text.includes('request B'))
})
test('residency merge bounded, expires own fragments without adding empty snapshot', async () => {
  const {ledger:l,tick}=await fixture({display:{maxChars:600,deltaMaxChars:600,maxResidentChars:800}})
  const d=new Delivery(l);await turn(l,'A')
  let p=await d.prepare('R',visible()),seen=[await accept(d,'R',p)]
  for(let n=0;n<4;n++) {
    await turn(l,`B${n}`)
    p=await d.prepare('R',visible(seen))
    assert.ok(p)
    if(p.replace) seen=[]
    seen.push(await accept(d,'R',p,visible(seen)))
    assert.ok(seen.reduce((n,m)=>n+m.text.length,0)<=800)
  }
  tick(25*3600000)
  const expire=await d.prepare('R',visible(seen))
  assert.equal(expire.kind,'retire');assert.equal(expire.text,'')
})
test('expired unconsumed interval explicitly recovers with history gap', async()=>{
  const {ledger:l,tick}=await fixture({retention:{mode:'ttl',durationMs:86400000,expiryAction:'delete'}})
  const d=new Delivery(l);await d.prepare('R',visible());await turn(l,'A');tick(86400001)
  await l.maintain({execute:true,authorization:'user-offline'});await turn(l,'B',1,1086400001)
  const p=await d.prepare('R',visible())
  assert.equal(p.kind,'recovery');assert.match(p.text,/history gap/)
})
test('residency consolidation never consumes undelivered backlog',async()=>{
 const {ledger:l}=await fixture({display:{maxRecords:1,maxChars:600,deltaMaxChars:600,deltaMaxEvents:1,maxResidentChars:800}})
 const d=new Delivery(l);await d.prepare('R',visible())
 for(const sid of ['A','B','C'])await turn(l,sid)
 let seen=[],observed=new Set(),p,iterations=0
 while((p=await d.prepare('R',visible(seen)))){
  p.recordIds.forEach(id=>observed.add(id))
  if(p.replace)seen=[]
  seen.push(await accept(d,'R',p,visible(seen)))
  assert.ok(++iterations<40)
 }
 assert.deepEqual([...observed].sort(),['A:1','B:1','C:1'])
 assert.equal(l.state.recipients.R.scanCursor,l.state.seq)
})
test('configuration shrink revalidates pending and consolidates existing residence without new events',async()=>{
 const {ledger:l}=await fixture({display:{maxChars:1000,deltaMaxChars:1000,maxResidentChars:2000}})
 for(const sid of ['A','B','C'])await turn(l,sid)
 const d=new Delivery(l),old=await d.prepare('R',visible())
 assert.ok(old.text.length>500)
 await l.updateConfig({'display.maxChars':500,'display.deltaMaxChars':500,'display.maxResidentChars':500},'shrink',1,{kind:'user-offline'})
 const p=await d.prepare('R',visible())
 assert.ok(p.text.length<=500)
 const m=await accept(d,'R',p)
 await l.updateConfig({'display.maxChars':600,'display.deltaMaxChars':600,'display.maxResidentChars':600},'expand',2,{kind:'user-offline'})
 await l.updateConfig({'display.maxChars':500,'display.deltaMaxChars':500,'display.maxResidentChars':500},'shrink',3,{kind:'user-offline'})
 const merge=await d.prepare('R',visible([{...m,text:m.text+'x'.repeat(200)}]))
 assert.equal(merge.replace,true);assert.ok(merge.text.length<=500)
})
test('committed delivery survives control write failure and acknowledgement retry',async()=>{
 const {ledger:l,store}=await fixture();await turn(l,'A');const d=new Delivery(l)
 const p=await d.prepare('R',visible()),m={deliveryId:p.deliveryId,text:p.text,recordIds:p.recordIds,createdAt:p.createdAt}
 store.failures=3
 await assert.rejects(d.prepare('R',visible([m])),/disk unavailable/)
 assert.equal(l.state.recipients.R.pending.deliveryId,p.deliveryId)
 store.failures=0
 assert.equal(await d.prepare('R',visible([m])),null)
 assert.equal(l.state.recipients.R.pending,null)
})
test('unacknowledged recovery remains recovery when configuration changes',async()=>{
 const {ledger:l}=await fixture();await turn(l,'A');const d=new Delivery(l)
 const p=await d.prepare('R',visible()),m=await accept(d,'R',p)
 const recovery=await d.prepare('R',visible([],[],42))
 assert.equal(recovery.kind,'recovery')
 await l.updateConfig({'display.maxRecords':2},'change during rejection',1,{kind:'user-offline'})
 const retry=await d.prepare('R',visible([],[],42))
 assert.equal(retry.kind,'recovery')
 assert.match(retry.text,/request A/)
})
test('rejected consolidation plus changed config restores baseline after owned nodes were retired',async()=>{
 const {ledger:l}=await fixture({display:{maxChars:600,deltaMaxChars:600,maxResidentChars:800}})
 await turn(l,'A');const d=new Delivery(l)
 let p=await d.prepare('R',visible()),m=await accept(d,'R',p)
 await l.note('A:1','a new title',{sessionId:'A'})
 p=await d.prepare('R',visible([{...m,text:m.text+'x'.repeat(500)}]))
 assert.equal(p.replace,true)
 // pre-step retired old owned nodes, then outer middleware vetoed admission.
 await l.updateConfig({'display.maxRecords':2},'change while rejected',1,{kind:'user-offline'})
 const retry=await d.prepare('R',visible())
 assert.equal(retry.kind,'baseline')
 assert.match(retry.text,/request A/)
 assert.equal(retry.through,l.state.recipients.R.scanCursor,'new events remain backlog')
})
