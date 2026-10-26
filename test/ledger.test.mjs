import test from 'node:test'
import assert from 'node:assert/strict'
import { Ledger } from '../src/ledger.js'
import { configFor } from '../src/config.js'
import { fixture, turn, event } from './fixtures.mjs'
test('one turn / ordered immutable events / duplicate replay / late clock / source distinction', async () => {
  const { ledger: l } = await fixture()
  await turn(l, 'A')
  const before = await l.list()
  assert.equal(before.records.length, 1)
  assert.equal(before.records[0].endedAt, 1000000000)
  for (const e of [event(1,'turn/start',{turn:1}), event(4,'turn/end',{turn:1,reason:{kind:'completed'}})]) await l.ingest('A',e)
  assert.equal(l.state.events.length, 4)
  await l.ingest('B', event(1, 'turn/start', {turn:1}, 1))
  assert.equal(l.state.events.at(-1).seq, 5)
  assert.equal(l.state.events.at(-1).occurredAt, 1)
  await l.ingest('B', event(2,'user/message',{source:{kind:'what-we-did'},content:[{type:'text',text:'injection'}]}))
  assert.equal(l.state.events.length,5)
  await l.note('A:1', '讨论方案', {sessionId:'B'})
  assert.equal((await l.get('A:1')).record.requestExcerpt,'request A')
  assert.equal(l.state.events.at(-1).kind,'title_annotated')
})
test('recovery preserves unknown end; ended mappings never invent success', async () => {
  const {ledger:l,store} = await fixture()
  await l.ingest('A',event(1,'turn/start',{turn:1}))
  const recovered = new Ledger(store,{},()=>1000000010)
  await recovered.ready
  const row = (await recovered.get('A:1')).record
  assert.equal(row.status,'interrupted'); assert.equal(row.endedAt,null)
  assert.equal(row.recoveryObservedAt,1000000010)
  for (const [reason,status] of [['blocked','unknown'],['max-tokens','unknown'],['error','failed'],['aborted','cancelled']]) {
    await recovered.ingest(reason,event(1,'turn/start',{turn:1}))
    await recovered.ingest(reason,event(2,'turn/end',{turn:1,reason:{kind:reason}}))
    assert.equal((await recovered.get(`${reason}:1`)).record.status,status)
  }
})
test('bounded snapshot pagination survives concurrent update and binds filters/timezone', async () => {
  const {ledger:l} = await fixture()
  for (const sid of ['A','B','C']) await turn(l,sid)
  const first = await l.list({limit:1})
  await l.note('B:1','changed',{sessionId:'A'})
  const second = await l.list({limit:1,cursor:first.nextCursor})
  const third = await l.list({limit:1,cursor:second.nextCursor})
  assert.equal(new Set([...first.records,...second.records,...third.records].map(r=>r.id)).size,3)
  await assert.rejects(l.list({sessionId:'X',cursor:first.nextCursor}),/cursor/)
  await assert.rejects(l.list({startTime:'2026-10-25'}),/timezone/)
  const combo = await l.list({sessionId:'A',startTime:new Date(999999999).toISOString(),endTime:new Date(1000000001).toISOString(),timeField:'startedAt'})
  assert.equal(combo.records.length,1)
})
test('configuration CAS permissions bounds audit cooldown and pending retention', async () => {
  const {ledger:l} = await fixture({modelAdjustment:{userLockedFields:['display.maxRecords'],minTurnsBetweenChanges:1}})
  await assert.rejects(l.updateConfig({'display.maxRecords':2},'reason',1,{kind:'model'}),/locked/)
  await assert.rejects(l.updateConfig({'display.maxChars':1},'reason',1,{kind:'model'}),/bound/)
  await assert.rejects(l.updateConfig({'display.lookbackHours':48},'reason',0,{kind:'model'}),/version/)
  const result=await l.updateConfig({'display.lookbackHours':48},'more context',1,{kind:'model',sessionId:'A'})
  assert.equal(result.configVersion,2)
  assert.equal(l.state.configAudit[0].reason,'more context')
  await assert.rejects(l.updateConfig({'display.lookbackHours':24},'reason',2,{kind:'model'}),/cooldown/)
  const pending=await l.updateConfig({'retention':{mode:'ttl',durationMs:86400000,expiryAction:'archive'},'display.lookbackHours':24},'shorten',2,{kind:'user'})
  assert.equal(pending.pendingConfirmation,true)
  assert.equal(l.state.config.retention.mode,'forever')
  assert.throws(()=>configFor({retention:{mode:'ttl',durationMs:100}}),/expiryAction/)
})
test('retention preview archive/delete are explicit, preserve running and explain gaps', async () => {
  const {ledger:l,tick} = await fixture({retention:{mode:'ttl',durationMs:86400000,expiryAction:'archive'}})
  await turn(l,'A'); await l.ingest('B',event(1,'turn/start',{turn:1}))
  tick(86400001)
  const preview=await l.maintain({execute:false})
  assert.deepEqual(preview.recordIds,['A:1'])
  assert.ok(l.state.records['A:1'])
  await l.maintain({execute:true,authorization:'user-offline'})
  assert.equal((await l.get('A:1')).error,'expired')
  assert.equal((await l.list()).records.length,1)
  assert.ok(l.state.archives['A:1']); assert.ok(l.state.gaps.length)
})
test('migration is faithful/idempotent; conflicting imports rejected; no fake old updates', async () => {
  const {ledger:l} = await fixture()
  const old={id:'legacy',sessionId:'S',turn:7,summary:'old request',reply:'old reply',taskIds:['T'],startedAt:1,endedAt:2,updatedAt:3,status:'replied',sourceSeqs:[10,11],requestSeqs:[10]}
  await l.importLegacy([old],{sha256:'a',source:'fixture'})
  await l.importLegacy([old],{sha256:'a',source:'fixture'})
  assert.equal(l.state.events.length,1); assert.equal(l.state.events[0].kind,'legacy_imported')
  const row=(await l.get('legacy')).record
  assert.equal(row.updatedAt,3); assert.equal(row.id,'legacy'); assert.deepEqual(row.legacy.taskIds,['T'])
  assert.deepEqual(row.legacy.original,old)
  await assert.rejects(l.importLegacy([{...old,reply:'changed'}],{sha256:'b'}),/conflict/)
})
test('failed persistence never exposes uncommitted projection, operation retries bounded', async () => {
  const {ledger:l,store}=await fixture()
  store.failures=3
  await assert.rejects(turn(l,'A'),/disk unavailable/)
  assert.equal(l.state.events.length,0)
  await assert.rejects(l.get('A:1'),/UNAVAILABLE/)
  store.failures=0
  await turn(l,'A')
  assert.equal(l.state.events.length,4)
})
test('retention tombstones prevent expired turn replay being attributed to an older retained turn',async()=>{
 const {ledger:l,tick}=await fixture({retention:{mode:'ttl',durationMs:86400000,expiryAction:'delete'}})
 await turn(l,'A',1);await turn(l,'A',5);tick(86400001)
 await l.note('A:1','keep older turn',{sessionId:'X'})
 await l.maintain({execute:true,authorization:'user-offline'})
 const before=structuredClone((await l.get('A:1')).record)
 await turn(l,'A',5)
 assert.deepEqual((await l.get('A:1')).record,before)
 assert.equal((await l.get('A:5')).error,'expired')
})
test('multi-step clarification remains one record; bounded excerpts preserve source addresses',async()=>{
 const {ledger:l}=await fixture()
 await l.ingest('S',event(1,'turn/start',{turn:1}))
 for(const [seq,type,data] of [[2,'user/message',{source:{kind:'user'},content:[{type:'text',text:'q'.repeat(10000)}]}],[3,'assistant/message',{message:{content:[{type:'text',text:'clarification?'}]}}],[4,'tool/call',{name:'task_get',arguments:'{"id":"not-a-link"}'}],[5,'user/message',{source:{kind:'user'},content:[{type:'text',text:'answer clarification'}]}],[6,'assistant/message',{message:{content:[{type:'text',text:'r'.repeat(10000)}]}}],[7,'turn/end',{turn:1,reason:{kind:'completed'}}]])await l.ingest('S',event(seq,type,data))
 const row=(await l.get('S:1')).record
 assert.equal((await l.list()).records.length,1);assert.equal(row.endedAt,1000000000)
 assert.ok(row.requestExcerpt.length<=480&&row.replyExcerpt.length<=360)
 assert.equal(row.requestTruncated,true);assert.equal(row.replyTruncated,true)
 assert.equal(row.sourceRefs.at(-1).eventSeq,7);assert.deepEqual(row.links,[])
})
test('finite retention exits ordinary query before explicit physical maintenance; reads never refresh update time',async()=>{
 const {ledger:l,tick}=await fixture({retention:{mode:'ttl',durationMs:86400000,expiryAction:'archive'}})
 await turn(l,'A');tick(86400001)
 assert.equal((await l.get('A:1')).error,'expired')
 assert.equal((await l.list()).records.length,0)
 assert.equal((await l.list({view:'events'})).events.length,0)
 assert.equal(l.state.records['A:1'].updatedAt,1000000000,'logical expiry is not physical deletion')
})
test('model cannot bypass a locked retention leaf by replacing its parent policy',async()=>{
 const {ledger:l}=await fixture({modelAdjustment:{allowedFields:['retention'],userLockedFields:['retention.durationMs']}})
 await assert.rejects(l.updateConfig({retention:{mode:'ttl',durationMs:86400000,expiryAction:'archive'}},'parent replacement',1,{kind:'model'}),/locked/)
 assert.equal(l.state.pendingPolicy,null)
})
