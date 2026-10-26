import { createHash, randomUUID } from 'node:crypto'
import { configFor, patched } from './config.js'
const clone = value => structuredClone(value)
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const rawText = content => (content??[]).filter(b=>b.type==='text').map(b=>b.text).join(' ').replace(/\s+/g,' ').trim()
const text = (content,max) => (content??[]).filter(b=>b.type==='text').map(b=>b.text).join(' ').replace(/\s+/g,' ').trim().slice(0,max)
export function project(events, expired = {}) {
  const rows={}
  for (const e of events) if (!expired[e.recordId]) rows[e.recordId]={...rows[e.recordId],...clone(e.patch)}
  return rows
}
export function newState(config={}) {
  return { format:1, seq:0, events:[], records:{}, recipients:{}, expired:{}, archives:{}, gaps:[], maintenance:[], config:configFor(config), configVersion:1, configAudit:[], completedHumanTurns:0, lastModelChangeTurn:null, pendingPolicy:null, ingestion:{observed:{},replyHeads:{}} }
}
/** Serial single-host transactions. Publication follows durable save; events rebuild projections. */
export class Ledger {
  constructor(store, config={}, clock=Date.now, activeSessionIds=[]) {
    this.store=store; this.clock=clock; this.queue=Promise.resolve(); this.failure=null
    this.ready=(async()=>{
      const stored=await store.load()
      this.state=stored??newState(config)
      if (this.state.format!==1) throw new Error('unsupported what we did format')
      this.state.config=configFor(this.state.config)
      this.state.records=project(this.state.events,this.state.expired)
      // 0.1.0 has no receipts for observations without a business change. Seed
      // only proven sources; never assume all seqs below a high-water were saved.
      this.state.ingestion??={observed:{},replyHeads:{}}
      for(const event of this.state.events) {
        const ref=event.sourceRef
        if(!ref||!Number.isSafeInteger(ref.eventSeq)||ref.eventSeq<0) continue
        const seen=this.state.ingestion.observed[ref.sessionId]??=[]
        if(!seen.includes(ref.eventSeq)) seen.push(ref.eventSeq)
        if(event.kind==='reply_captured'&&ref.kind==='assistant/message'&&this.state.records[event.recordId]?.sessionId===ref.sessionId&&event.patch.replyExcerpt?.trim()) {
          this.state.ingestion.replyHeads[event.recordId]=Math.max(this.state.ingestion.replyHeads[event.recordId]??-1,ref.eventSeq)
        }
      }
      for(const seen of Object.values(this.state.ingestion.observed)) seen.sort((a,b)=>a-b)
      if (stored && Object.keys(config).length) {
        // Deployment settings are the user authority; an explicit reload restores their values.
        const effective=configFor({...this.state.config,...config,display:{...this.state.config.display,...config.display},modelAdjustment:{...this.state.config.modelAdjustment,...config.modelAdjustment}})
        if (JSON.stringify(effective)!==JSON.stringify(this.state.config)) {
          this.state.configAudit.push({actor:{kind:'deployment'},reason:'explicit plugin configuration on load',old:clone(this.state.config),new:effective,effectiveAt:clock(),configVersion:++this.state.configVersion})
          this.state.config=effective
        }
      }
      for (const r of Object.values(this.state.records)) if(r.status==='running'&&!activeSessionIds.includes(r.sessionId)) this.append(this.state,r.id,`recovery:${r.id}:${r.revision}`,'turn_interrupted',{status:'interrupted',recoveryObservedAt:clock()},clock())
      // Live sessions supplied by the plugin are not mistaken for crash orphans.
      await store.save(this.state)
    })()
  }
  async tx(fn) {
    const operation=this.queue.then(async()=>{
      await this.ready
      const next=clone(this.state)
      const result=await fn(next)
      if(JSON.stringify(next)===JSON.stringify(this.state)) return result
      for(let attempt=0;;attempt++) {
        try { await this.store.save(next); break }
        catch(e) { this.failure=String(e); if(attempt===2) throw e }
      }
      this.state=next; this.failure=null
      return result
    })
    this.queue=operation.catch(()=>{})
    return operation
  }
  async flush() { await this.ready; await this.queue; if(this.failure) throw new Error(`UNAVAILABLE: ${this.failure}`) }
  append(s,id,eventId,kind,patch,occurredAt,sourceRef, preserveUpdate=false) {
    if(s.events.some(e=>e.eventId===eventId)) return false
    const prev=s.records[id]
    const revision=(prev?.revision??0)+1
    const change={...clone(patch),revision, ...(preserveUpdate?{}:{updatedAt:this.clock()})}
    const event={eventId,seq:++s.seq,occurredAt,recordedAt:this.clock(),recordId:id,recordRevision:revision,kind,patch:change,...(sourceRef?{sourceRef}: {})}
    s.events.push(event); s.records[id]={...prev,...change}
    return true
  }
  async ingest(sessionId,e,turnStartSeq) {
    return this.tx(s=>{
      const eventId=`${sessionId}:${e.seq}:${e.type}`
      if(!Number.isSafeInteger(e.seq)||e.seq<0) throw new Error('invalid source seq')
      const observed=s.ingestion.observed[sessionId]??=[]
      if(observed.includes(e.seq)) return
      const markObserved=()=>{observed.push(e.seq);observed.sort((a,b)=>a-b)}
      if(s.events.some(x=>x.eventId===eventId)) {markObserved();return}
      const anchors=[...Object.values(s.records).filter(r=>!r.legacy).map(r=>({id:r.id,sessionId:r.sessionId,startSeq:r.sourceRefs[0]?.eventSeq})),...Object.values(s.expired)]
      const anchor=anchors.filter(r=>r.sessionId===sessionId&&r.startSeq<=e.seq).sort((a,b)=>b.startSeq-a.startSeq)[0]
      const recordId=turnStartSeq===undefined?(anchor?.id??anchor?.recordId):`${sessionId}:${turnStartSeq}`
      let row=recordId?s.records[recordId]:undefined
      const sourceRef={sessionId,eventSeq:e.seq,kind:e.type}
      let kind,patch,id=row?.id
      if(e.type==='turn/start') {
        id=`${sessionId}:${e.seq}`
        if(s.records[id] || s.expired[id]) {markObserved();return}
        kind='turn_started'
        patch={id,sessionId,turnKey:id,turnNumber:e.data.turn,startedAt:e.time,endedAt:null,status:'running',requestExcerpt:'',replyExcerpt:'',sourceRefs:[sourceRef],links:[],humanInitiated:false}
      } else {
        if(!row) return
        // A missing newer start must not redirect sources into an older turn.
        const closedAt=s.events.findLast(x=>x.recordId===row.id&&x.kind==='turn_finished'&&x.sourceRef?.kind==='turn/end')?.sourceRef.eventSeq
        if(closedAt!==undefined&&e.seq>closedAt) return
        if(e.data.turn!==undefined&&e.data.turn!==row.turnNumber) return
        // Receipt and business update commit together, including no-op sources.
        markObserved()
        patch={sourceRefs:[...row.sourceRefs,sourceRef]}
        if(e.type==='user/message') {
          if(e.data.source.kind!=='user') return
          const excerpt=text(e.data.content,240)||'(非文本请求)'
          patch.requestExcerpt=(row.requestExcerpt?`${row.requestExcerpt}；`:'')+excerpt
          patch.requestTruncated=Boolean(row.requestTruncated||rawText(e.data.content).length>240||patch.requestExcerpt.length>480)
          patch.requestExcerpt=patch.requestExcerpt.slice(0,480)
          patch.humanInitiated=true; kind='request_captured'
        } else if(e.type==='assistant/message') {
          const reply=text(e.data.message.content,360)
          if(!reply) return // Tool/reasoning/whitespace messages never erase text or truncation.
          const head=s.ingestion.replyHeads[id]??-1
          if(e.seq<head) return
          s.ingestion.replyHeads[id]=e.seq
          const truncated=rawText(e.data.message.content).length>360
          if(reply===row.replyExcerpt&&truncated===Boolean(row.replyTruncated)) return
          patch.replyExcerpt=reply;patch.replyTruncated=rawText(e.data.message.content).length>360;kind='reply_captured'
        } else if(e.type==='turn/end') {
          if(e.data.turn!==row.turnNumber) return
          const reason=e.data.reason.kind
          patch.endReason=reason
          patch.endedAt=reason==='interrupted'?null:e.time
          patch.status=({completed:'replied',error:'failed',aborted:'cancelled',interrupted:'interrupted'})[reason]??'unknown'
          if(reason==='interrupted') patch.recoveryObservedAt=this.clock()
          kind='turn_finished'
          if(row.humanInitiated) s.completedHumanTurns++
        } else return
      }
      if(e.type==='turn/start')markObserved()
      this.append(s,id,eventId,kind,patch,e.time,sourceRef)
    })
  }
  async recover(activeSessionIds=[]) {
    return this.tx(s=>{
      for(const r of Object.values(s.records)) if(r.status==='running'&&!activeSessionIds.includes(r.sessionId)) this.append(s,r.id,`recovery:${r.id}:${r.revision}`,'turn_interrupted',{status:'interrupted',recoveryObservedAt:this.clock()},this.clock())
    })
  }
  async get(id) {
    await this.flush()
    const expired=this.expiring(this.state).find(r=>r.id===id)
    if(expired) return {error:'expired',recordId:id,reason:this.state.config.retention.expiryAction,physicalMaintenancePending:true}
    return this.state.records[id]?{record:clone(this.state.records[id])}:this.state.expired[id]?{error:'expired',...this.state.expired[id]}:{error:'not_found'}
  }
  async note(id,title,source) {
    if(typeof title!=='string'||!title.trim()||title.length>240) throw new Error('title must be 1..240 characters')
    return this.tx(s=>{
      if(!s.records[id]) throw new Error('record not found')
      this.append(s,id,`note:${randomUUID()}`,'title_annotated',{displayTitle:title,annotationSource:source},this.clock())
      return clone(s.records[id])
    })
  }
  async list(filters={}) {
    await this.flush()
    const {cursor,...raw}=filters
    const f={view:'records',order:'desc',timeField:'updatedAt',limit:20,...raw}
    if(!['records','events'].includes(f.view)||!['asc','desc'].includes(f.order)||!['updatedAt','startedAt','endedAt'].includes(f.timeField)||!Number.isInteger(f.limit)||f.limit<1||f.limit>100) throw new Error('invalid query')
    for(const key of ['startTime','endTime']) if(f[key]!==undefined && (!/(Z|[+-]\d{2}:\d{2})$/.test(f[key])||!Number.isFinite(Date.parse(f[key])))) throw new Error('time requires ISO timezone')
    if(f.startTime&&f.endTime&&Date.parse(f.startTime)>=Date.parse(f.endTime)) throw new Error('invalid time range')
    const binding=digest(f)
    let c={h:this.state.seq,offset:0,binding,gap:this.state.gaps.length,at:this.clock()}
    if(cursor) { try { c=JSON.parse(Buffer.from(cursor,'base64url').toString()) } catch { throw new Error('invalid cursor') }
      if(c.binding!==binding||!Number.isSafeInteger(c.h)||c.h<0||c.h>this.state.seq||!Number.isSafeInteger(c.offset)||c.offset<0||c.gap!==this.state.gaps.length||!Number.isFinite(c.at)) throw new Error('cursor mismatch or expired')
    }
    const events=this.state.events.filter(e=>e.seq<=c.h)
    const projected=project(events,this.state.expired)
    const expiredAt=time=>this.expiring({...this.state,records:projected},this.state.config,time).map(r=>r.id).sort()
    const expiredNow=expiredAt(this.clock())
    if(cursor&&JSON.stringify(expiredNow)!==JSON.stringify(expiredAt(c.at))) throw new Error('cursor expired at retention boundary; restart query')
    let rows=f.view==='records'?Object.values(projected).filter(r=>!expiredNow.includes(r.id)):events.filter(e=>!expiredNow.includes(e.recordId))
    const start=f.startTime?Date.parse(f.startTime):-Infinity,end=f.endTime?Date.parse(f.endTime):Infinity
    rows=rows.filter(r=>{
      const sid=f.view==='records'?r.sessionId:r.patch.sessionId??this.state.records[r.recordId]?.sessionId
      const time=f.view==='records'?r[f.timeField]:r.occurredAt
      return (!f.sessionId||sid===f.sessionId)&&(!f.startTime&&!f.endTime || time!==null&&time>=start&&time<end)
    })
    rows.sort((a,b)=>{const delta=f.view==='records'?(a[f.timeField]??0)-(b[f.timeField]??0)||a.id.localeCompare(b.id):a.seq-b.seq;return f.order==='asc'?delta:-delta})
    const page=[]; let chars=0,truncated=false
    for(const row of rows.slice(c.offset,c.offset+f.limit)) {
      const short=f.view==='records'?Object.fromEntries(Object.entries(row).filter(([k])=>!['sourceRefs','legacy','annotationSource'].includes(k))):{...row,patch:{...row.patch,sourceRefs:undefined,legacy:undefined}}
      const length=JSON.stringify(short).length
      if(chars+length>16000&&page.length) {truncated=true;break}
      // Imported legacy payloads can be arbitrarily long: detail retains the original.
      if(length>16000) { page.push({id:row.id??row.recordId,seq:row.seq,truncated:true,detail:'did_get'}); truncated=true } else page.push(short)
      chars+=Math.min(length,16000)
    }
    const offset=c.offset+page.length,hasMore=offset<rows.length
    return {[f.view]:clone(page),nextCursor:hasMore?Buffer.from(JSON.stringify({...c,offset})).toString('base64url'):null,hasMore,truncated,range:f,highWaterSeq:c.h,queryAt:c.at,retention:clone(this.state.config.retention),historyGaps:this.state.gaps.length}
  }
  async updateConfig(patch,reason,expectedVersion,actor={kind:'model'}) {
    if(!reason?.trim()||reason.length>1000) throw new Error('reason required, maximum 1000 chars')
    return this.tx(s=>{
      if(expectedVersion!==s.configVersion) throw new Error('config version conflict')
      const m=s.config.modelAdjustment
      if(actor.kind==='model') {
        if(!m.enabled) throw new Error('model adjustment disabled')
        for(const [key,v] of Object.entries(patch)) {
          if(m.userLockedFields.includes(key)||m.userLockedFields.some(k=>key.startsWith(`${k}.`)||k.startsWith(`${key}.`))) throw new Error(`user locked ${key}`)
          if(!m.allowedFields.includes(key)) throw new Error(`field not allowed ${key}`)
          const b=m.bounds[key]; if(b&&(v<b.min||v>b.max)) throw new Error(`outside bound ${key}`)
        }
        if(s.lastModelChangeTurn!==null&&s.completedHumanTurns-s.lastModelChangeTurn<m.minTurnsBetweenChanges) throw new Error('model adjustment cooldown')
      }
      const next=patched(s.config,patch)
      const shorter=next.retention.mode==='ttl'&&(s.config.retention.mode==='forever'||next.retention.durationMs<s.config.retention.durationMs||next.retention.expiryAction!==s.config.retention.expiryAction)
      if(shorter) {
        s.pendingPolicy={patch:clone(patch),reason,actor:clone(actor),expectedVersion,impact:this.expiring(s,next).map(r=>r.id)}
        return {pendingConfirmation:true,effective:false,...clone(s.pendingPolicy)}
      }
      const old=clone(s.config);s.config=next;s.configVersion++
      const audit={old,new:clone(next),actor:clone(actor),reason,effectiveAt:this.clock(),configVersion:s.configVersion}
      s.configAudit.push(audit)
      if(actor.kind==='model') s.lastModelChangeTurn=s.completedHumanTurns
      return {...clone(audit),pendingConfirmation:false}
    })
  }
  async confirmPolicy(expectedVersion) {
    return this.tx(s=>{
      if(!s.pendingPolicy||expectedVersion!==s.configVersion||s.pendingPolicy.expectedVersion!==expectedVersion) throw new Error('pending policy version conflict')
      const p=s.pendingPolicy,old=clone(s.config)
      s.config=patched(s.config,p.patch);s.configVersion++;s.pendingPolicy=null
      s.configAudit.push({old,new:clone(s.config),actor:{kind:'user-offline'},reason:p.reason,effectiveAt:this.clock(),configVersion:s.configVersion})
      return {configVersion:s.configVersion}
    })
  }
  expiring(s,config=s.config,at=this.clock()) {
    if(config.retention.mode==='forever') return []
    return Object.values(s.records).filter(r=>r.status!=='running'&&Math.max(r.endedAt??0,r.updatedAt,r.recoveryObservedAt??0)+config.retention.durationMs<at)
  }
  async maintain({execute=false,authorization}={}) {
    return this.tx(s=>{
      const rows=this.expiring(s),recordIds=rows.map(r=>r.id),action=s.config.retention.expiryAction
      if(!execute) return {recordIds,action,executionRequires:'offline backup and explicit user authorization'}
      if(authorization!=='user-offline') throw new Error('maintenance requires explicit user authorization')
      const seqs=s.events.filter(e=>recordIds.includes(e.recordId)).map(e=>e.seq)
      for(const r of rows) {
        if(action==='archive') s.archives[r.id]={record:clone(r),events:s.events.filter(e=>e.recordId===r.id)}
        s.expired[r.id]={recordId:r.id,sessionId:r.sessionId,startSeq:r.legacy?null:r.sourceRefs[0]?.eventSeq,expiredAt:this.clock(),reason:action}
        delete s.records[r.id]
      }
      s.events=s.events.filter(e=>!recordIds.includes(e.recordId))
      if(seqs.length) s.gaps.push({seqs,at:this.clock(),reason:action})
      const result={recordIds,action,at:this.clock()};s.maintenance.push(result);return result
    })
  }
  /** Inspect immutable reply evidence; only explicit offline execution appends corrections. */
  async repairReplies({execute=false,reason,recordIds}={}) {
    if(execute&&(!reason?.trim()||reason.length>1000)) throw new Error('repair reason required, maximum 1000 chars')
    return this.tx(s=>{
      const expired=new Set(this.expiring(s).map(r=>r.id))
      const ids=recordIds??[...new Set([...Object.keys(s.records),...Object.keys(s.expired),...Object.keys(s.archives)])]
      const report={scanned:ids.length,emptyExcerptCount:0,repairableCount:0,repairedCount:0,unresolvedCount:0,unchangedCount:0,skippedCount:0,records:[]}
      for(const id of ids) {
        const row=s.records[id],item={recordId:id,oldLength:row?.replyExcerpt.length??null}
        report.records.push(item)
        if(!row||s.expired[id]||s.archives[id]||expired.has(id)) {
          item.status='skipped';item.reason=s.expired[id]?.reason??(s.archives[id]?'archived':expired.has(id)?'expired':'not_found');report.skippedCount++;continue
        }
        if(!row.replyExcerpt) report.emptyExcerptCount++
        const events=s.events.filter(e=>e.recordId===id)
        const start=events.find(e=>e.kind==='turn_started')?.sourceRef?.eventSeq
        const end=events.find(e=>e.kind==='turn_finished'&&e.sourceRef?.kind==='turn/end')?.sourceRef?.eventSeq??Infinity
        const candidates=events.filter(e=>e.kind==='reply_captured'&&typeof e.patch.replyExcerpt==='string'&&e.patch.replyExcerpt.trim())
        const valid=e=>Number.isSafeInteger(start)&&e.sourceRef?.sessionId===row.sessionId&&e.sourceRef.kind==='assistant/message'&&Number.isSafeInteger(e.sourceRef.eventSeq)&&e.sourceRef.eventSeq>start&&e.sourceRef.eventSeq<end&&e.patch.replyExcerpt.length<=360&&typeof e.patch.replyTruncated==='boolean'&&Number.isFinite(e.occurredAt)
        if(!candidates.length||candidates.some(e=>!valid(e))) {
          item.status='unresolved';item.reason=candidates.length?'unverifiable_reply_source':'no_nonempty_reply_evidence';report.unresolvedCount++;continue
        }
        candidates.sort((a,b)=>b.sourceRef.eventSeq-a.sourceRef.eventSeq||a.seq-b.seq)
        const evidence=candidates[0]
        if(candidates.some(e=>e.sourceRef.eventSeq===evidence.sourceRef.eventSeq&&(e.patch.replyExcerpt!==evidence.patch.replyExcerpt||e.patch.replyTruncated!==evidence.patch.replyTruncated))) {
          item.status='unresolved';item.reason='conflicting_reply_source';report.unresolvedCount++;continue
        }
        item.newLength=evidence.patch.replyExcerpt.length
        item.evidence={eventId:evidence.eventId,ledgerSeq:evidence.seq,sourceRef:clone(evidence.sourceRef),occurredAt:evidence.occurredAt}
        if(row.replyExcerpt===evidence.patch.replyExcerpt&&Boolean(row.replyTruncated)===evidence.patch.replyTruncated) {
          item.status='unchanged';report.unchangedCount++;continue
        }
        const repairId=`repair:reply:${digest({id,evidence:item.evidence,excerpt:evidence.patch.replyExcerpt,truncated:evidence.patch.replyTruncated})}`
        if(s.events.some(e=>e.eventId===repairId)) {
          item.status='unresolved';item.reason='previous_correction_no_longer_matches';report.unresolvedCount++;continue
        }
        report.repairableCount++;item.status=execute?'repaired':'repairable';item.repairId=repairId
        if(!execute) continue
        this.append(s,id,repairId,'reply_corrected',{replyExcerpt:evidence.patch.replyExcerpt,replyTruncated:evidence.patch.replyTruncated,updatedAt:row.updatedAt},evidence.occurredAt,evidence.sourceRef,true)
        s.events.at(-1).correction={reason,processedAt:this.clock(),evidence:clone(item.evidence)}
        s.ingestion.replyHeads[id]=Math.max(s.ingestion.replyHeads[id]??-1,evidence.sourceRef.eventSeq)
        report.repairedCount++
      }
      return report
    })
  }
  async importLegacy(rows,origin) {
    return this.tx(s=>{
      let imported=0
      for(const old of rows) {
        if(typeof old.id!=='string'||typeof old.sessionId!=='string'||!Number.isFinite(old.startedAt)||!Number.isFinite(old.updatedAt)||!(old.endedAt===null||Number.isFinite(old.endedAt))||typeof old.summary!=='string'||typeof old.reply!=='string'||!Array.isArray(old.sourceSeqs)||!['running','replied','failed','cancelled','unknown','interrupted'].includes(old.status)) throw new Error('invalid legacy record')
        if(s.expired[old.id]) throw new Error('legacy record already expired')
        if(s.records[old.id]) { if(digest(s.records[old.id].legacy?.original)!==digest(old)) throw new Error(`legacy conflict ${old.id}`); continue }
        const row={id:old.id,sessionId:old.sessionId,turnKey:old.id,turnNumber:old.turn,startedAt:old.startedAt,endedAt:old.endedAt,updatedAt:old.updatedAt,status:old.status,requestExcerpt:old.summary,replyExcerpt:old.reply,sourceRefs:old.sourceSeqs.map(eventSeq=>({sessionId:old.sessionId,eventSeq,kind:'legacy_unknown'})),links:[],legacy:{original:clone(old),taskIds:old.taskIds??[],importId:origin.sha256,origin:clone(origin),missingEventHistory:true}}
        this.append(s,old.id,`legacy:${old.id}`,'legacy_imported',row,old.updatedAt,undefined,true);imported++
      }
      return {imported,total:rows.length}
    })
  }
}
