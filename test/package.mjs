import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync,rmSync,readFileSync,mkdirSync,symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join,resolve } from 'node:path'
import { fileURLToPath,pathToFileURL } from 'node:url'
import { boot,dep,host,send } from './host.mjs'
const root=fileURLToPath(new URL('../',import.meta.url)),temporary=mkdtempSync(join(tmpdir(),'wwd-package-'))
try{
 const result=JSON.parse(execFileSync('npm',['pack','--ignore-scripts','--json','--cache',join(temporary,'npm-cache'),'--pack-destination',temporary],{cwd:root,encoding:'utf8'}))[0]
 const names=result.files.map(f=>f.path)
 for(const required of ['src/index.js','src/host-store.js','skills/what-we-did/SKILL.md','scripts/manage.mjs','types/index.d.ts','config.example.json','config.schema.json','README.md','cordis.patch.yml'])assert.ok(names.includes(required),required)
 assert.ok(names.every(n=>!n.startsWith('node_modules/')&&!n.startsWith('test/')))
 execFileSync('tar',['-xzf',join(temporary,result.filename),'-C',temporary])
 const installed=join(temporary,'package')
 // The real profile supplies these exact existing host peers, no registry/network needed.
 symlinkSync(join(root,'node_modules'),join(installed,'node_modules'),'dir')
 const plugin=await import(pathToFileURL(join(installed,'src/index.js')))
 const {ctx,agent:create,adapter}=await boot()
 try{
  for(const module of ['storage/storage','storage/storage-json','storage/storage-domain'])await ctx.plugin((await host(module)).default??await host(module),module.endsWith('storage-json')?{root:join(temporary,'storage')}:module.endsWith('storage-domain')?{backend:'json'}:{})
  await ctx.plugin((await host('skill/skill')).default)
  await ctx.plugin(plugin,{})
  const skill=await ctx.skills.get('what-we-did')
  assert.ok(skill.content.includes('we代表用户与Agent'))
  const a=await create('packaged-A'),b=await create('packaged-B')
  await send(a,'package source');await send(b,'package recipient')
  assert.ok(adapter.requests.at(-1).messages.some(m=>m.source?.kind==='what-we-did'))
 }finally{await ctx.fiber.dispose()}
 console.log(`PACKAGE_OK: ${result.filename}, ${names.length} files; real host/skill/independent input from extracted tgz`)
}finally{rmSync(temporary,{recursive:true,force:true})}
