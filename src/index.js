import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import Schema from '@deepseek-ai/schemastery'
import { createMessage, createUserMessage } from '@deepseek-ai/dsh-llm'
import { defineTool } from '@deepseek-ai/dsh-tools'
import { Ledger } from './ledger.js'
import { Delivery } from './delivery.js'
import { hostStore } from './host-store.js'
export const name='what-we-did'
export const inject=['storageDomain','tools','systemPrompt','agents','sessions']
export const Config=Schema.object({retention:Schema.any(),display:Schema.any(),modelAdjustment:Schema.any()})
export const SYSTEM_INSTRUCTIONS=`what we did（we代表用户与Agent）自动记录每个真实轮次的请求/回复摘录、时间、状态与来源。普通轮次不用调用记录工具。
自动上下文是有限基线或增量，按id和revision与此前仍有效片段合并理解；最后一块不是完整历史，未出现不代表删除。需要时间线或会话线时使用did_list，再用did_get读元数据与来源。未展示不等于未记录，空查询须检查范围与保存边界。
空回复摘录仅表示did没有可用摘录，不能推断原会话未保存、已丢失或无法恢复。sourceRefs是定位信息，不是已读取全文的证明；用户索要原话时须有实际原会话读取能力，否则说明当前工具不能直接读取全文，不拿文档段落冒充聊天原话。
请求、助手建议、正常回复均不等于真实执行、业务完成或用户授权；did摘录不是执行证明。相邻轮次不自动属于同一任务。标题注解不覆盖原文，也不能提升事实等级。
先用did_config_get核对用户锁定、模型权限、边界和版本，再按理由调整许可字段；保留审计，不把调参理由当用户长期偏好。完整用法见随插件分发的skills/what-we-did/SKILL.md。`
export function visibleHistory(session) {
  const messages=[],originals=[]
  let replacement=0
  for(const e of session.snapshotEvents()) {
    const source=e.type==='user/message'?e.data.source:e.data.message?.source
    if(e.surfaceOp && e.surfaceOp!=='append' && source?.kind!=='what-we-did' && e.type!=='system/message') replacement=e.seq
  }
  for(const seq of session.surface.nodes) {
    const e=session.eventAt(seq)
    if(e.type==='user/message'&&e.data.source.kind==='what-we-did') messages.push({seq,...e.data.source,text:e.data.content.filter(b=>b.type==='text').map(b=>b.text).join('')})
    else if(e.type==='user/message'&&e.data.source.kind==='user'||e.type==='assistant/message') originals.push(seq)
  }
  const events=session.snapshotEvents()
  const start=events.findLast(e=>e.type==='turn/start')
  const currentRecordId=start&&!events.some(e=>e.seq>start.seq&&e.type==='turn/end'&&e.data.turn===start.data.turn)?`${session.id}:${start.seq}`:null
  return {messages,originals,replacement,currentRecordId}
}
function retire(session,nodes,turn,step) {
  for(const node of nodes) {
    if(!session.surface.nodes.includes(node.seq)) continue
    // Empty developer nodes project to no wire message. Replace exactly our own node.
    session.append('developer/message',{turn,step,message:createMessage({role:'developer',content:[],source:{kind:'what-we-did',retired:true}})},{surfaceOp:{op:'replace',startSeq:node.seq,endSeq:node.seq},sourceEventSeqs:[node.seq]})
  }
}
/** Automatic recording and isolated message admission; never drives or wakes an Agent. */
export function apply(ctx,config={}) {
  for(const tool of ['did_list','did_get','did_note','did_config_get','did_config_update','did_diagnostics']) if(ctx.tools.get(tool)) throw new Error(`what we did tool conflict: ${tool}; disable old task list didLog before enabling`)
  const store=hostStore(ctx)
  const active=ctx.agents.list().filter(a=>a.status==='running').map(a=>String(a.session.id))
  const ledger=new Ledger(store,config,Date.now,active),delivery=new Delivery(ledger)
  let error=null,closed=false
  void ledger.ready.catch(e=>{error=String(e);ctx.logger.error(`what we did UNAVAILABLE: ${error}`)})
  const track = promise => { void promise.catch(e=>{error=String(e);ctx.logger.error(`what we did UNAVAILABLE (source replay on next input): ${error}`)}) }
  ctx.effect(()=>async()=>{closed=true;try{await ledger.flush()}finally{await store.close()}},'what-we-did flush/close')
  ctx.on('session/event',(session,e)=>{
    if(closed||session.header.origin==='subagent') return
    if(['turn/start','user/message','assistant/message','turn/end'].includes(e.type)) track(ledger.ingest(String(session.id),e,session.snapshotEvents().findLast(source=>source.type==='turn/start'&&source.seq<=e.seq)?.seq))
  })
  ctx.systemPrompt.section({name:'what-we-did:instructions',order:ctx.systemPrompt.getSectionOrder('TOOL_SUBAGENT'),interpolate:false,text:SYSTEM_INSTRUCTIONS})
  ctx.on('agent/pre-step',async(payload,next)=>{
    const decision=await next()
    if(decision.kind!=='enter'||payload.step!==1||payload.agent.session.header.origin==='subagent') return decision
    const session=payload.agent.session,sid=String(session.id)
    // Replay repairs missed events after a transient write failure or late plugin attachment.
    let turnStartSeq
    for(const e of session.snapshotEvents()) {
      if(e.type==='turn/start') turnStartSeq=e.seq
      if(['turn/start','user/message','assistant/message','turn/end'].includes(e.type)) await ledger.ingest(sid,e,turnStartSeq)
    }
    await ledger.flush();error=null
    let visible=visibleHistory(session)
    const committedMerge=visible.messages.findLast(m=>m.replace)
    if(committedMerge) {retire(session,visible.messages.filter(m=>m.seq<committedMerge.seq),payload.turn,payload.step);visible=visibleHistory(session)}
    const p=await delivery.prepare(sid,visible)
    if(!p) return decision
    payload.signal.throwIfAborted()
    if(p.kind==='retire') {retire(session,visible.messages,payload.turn,payload.step);return decision}
    const source={kind:'what-we-did',deliveryId:p.deliveryId,epoch:p.epoch,from:p.from,through:p.through,createdAt:p.createdAt,recordIds:p.recordIds,replace:p.replace}
    const message=createUserMessage({content:[{type:'text',text:p.text}],source})
    // Session observers cannot append reentrantly. Retire owned nodes here,
    // after the full replacement batch is durable but before request admission.
    // A veto/cancellation leaves that same pending baseline available to retry.
    if(p.replace) retire(session,visible.messages,payload.turn,payload.step)
    // Appended through the loop's admitted messages. No direct append before acceptance.
    return {...decision,messages:[...decision.messages,message]}
  })
  // Submission evidence is the effective surface, not a model answer. The follow-up
  // reconciliation also handles committed-message/control-write crash windows.
  ctx.on('session/event',(session,e)=>{
    if(e.type!=='user/message'||e.data.source.kind!=='what-we-did'||closed) return
    track(delivery.prepare(String(session.id),visibleHistory(session)))
  })
  ctx.inject(['skills'], inner=>{
    const path=fileURLToPath(new URL('../skills/what-we-did/SKILL.md',import.meta.url))
    const content=readFileSync(path,'utf8').replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/,'')
    inner.skills.register({name:'what-we-did',description:'用户与Agent近期经历、时间/会话查询、增量理解与窗口调整',source:'bundled',path,content,resourceBase:{kind:'directory',path:fileURLToPath(new URL('../skills/what-we-did/',import.meta.url))}})
  })
  const output={schema:{type:'string'},render:(_args,value)=>[{type:'text',text:value}]}
  const register=(name,description,parameters,execute)=>ctx.tools.register(defineTool({name,description,parameters,output,async execute(args,exec){await ledger.flush();return JSON.stringify(await execute(args,exec))}}))
  const string=description=>({type:'string',description})
  register('did_list','Page retained what we did history. Default records, updatedAt descending, 20 rows, max 100 and 16000 chars. Time requires ISO timezone, [start,end). Session/time filters intersect. events filters occurredAt and orders seq. Cursor binds filters and fixed high-water; omissions/retention are explicit. Excerpts are not execution proof. An empty reply excerpt does not mean the source conversation is absent or lost; source references do not grant full-text access.',{
    view:{type:'string',enum:['records','events']},sessionId:string('Exact source session ID'),startTime:string('Inclusive ISO 8601 with timezone'),endTime:string('Exclusive ISO 8601 with timezone'),timeField:{type:'string',enum:['updatedAt','startedAt','endedAt']},order:{type:'string',enum:['asc','desc']},limit:{type:'number'},cursor:string('nextCursor from identical filters, including limit')},args=>ledger.list(args))
  register('did_get','Get full did metadata and source addresses, not full conversation text. Empty replyExcerpt does not establish loss of the original conversation. Reading original wording requires an actual source-reading capability. Distinguishes expired and not_found. For a large legacy payload, page JSON characters with offset/limit; truncated and nextOffset are explicit.',{id:{type:'string',required:true},offset:{type:'number'},limit:{type:'number'}},async({id,offset=0,limit=16000})=>{
    if(!Number.isInteger(offset)||offset<0||!Number.isInteger(limit)||limit<1||limit>16000) throw new Error('invalid detail page')
    const result=await ledger.get(id),raw=JSON.stringify(result)
    return raw.length<=limit&&offset===0?result:{id,jsonFragment:raw.slice(offset,offset+limit),nextOffset:offset+limit<raw.length?offset+limit:null,truncated:true,totalChars:raw.length}
  })
  register('did_note','Optional short model title annotation for this turn or exact id. Automatic recording needs no tool. Preserves original request; cannot change status, execution evidence or task ownership.',{id:string('Explicit record ID; omitted uses current active turn'),title:{type:'string',required:true}},async({id,title},exec)=>{
    const sid=String(exec.agent?.session.id??'')
    id??=Object.values(ledger.state.records).findLast(r=>r.sessionId===sid&&r.status==='running')?.id
    return ledger.note(id,title,{kind:'model',sessionId:sid})
  })
  register('did_config_get','Read effective global config, model permissions, locks, CAS version, recent audit, cooldown and pending retention policy. Saving/display are independent.',{},()=>({config:ledger.state.config,configVersion:ledger.state.configVersion,audit:ledger.state.configAudit.slice(-10),pendingPolicy:ledger.state.pendingPolicy,completedHumanTurns:ledger.state.completedHumanTurns,lastModelChangeTurn:ledger.state.lastModelChangeTurn}))
  register('did_config_update','Apply allowed global configuration leaf patch with reason and expectedVersion. Dot keys e.g. display.lookbackHours. User locks/bounds/cooldown enforced. Shortened retention returns pending policy/impact; never physically deletes. No self-wakeup.',{patch:{type:'object',additionalProperties:true,required:true},reason:{type:'string',required:true},expectedVersion:{type:'number',required:true}},(args,exec)=>ledger.updateConfig(args.patch,args.reason,args.expectedVersion,{kind:'model',sessionId:String(exec.agent?.session.id??'')}))
  register('did_diagnostics','Read delivery/config diagnostics for one recipient session; no global conversation export. Pending is not acknowledged. Logs bounded to last 50 deliveries.',{sessionId:{type:'string',required:true}},({sessionId})=>({recipient:ledger.state.recipients[sessionId]??null,configVersion:ledger.state.configVersion,highWaterSeq:ledger.state.seq,unavailable:error??ledger.failure,historyGaps:ledger.state.gaps.length}))
}
