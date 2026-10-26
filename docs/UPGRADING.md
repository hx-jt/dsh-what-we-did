# 升级与离线维护

## 从0.1.0升级

0.1.1修复旧工具/空正文消息或相同进度文本在历史回放中覆盖最终回复的问题。新增控制字段ingestion，兼容原format=1数据；观察凭据不制造业务更新。写失败可重试，无法确定轮次归属的来源不提前消费。

现存空摘录不会在启动时自动重写。先备份并在副本运行检查；有同一did的不可变有效回复证据才追加纠正事件。不能从其他轮次、任务、ReMe或文档猜内容。

以下命令在已解包的插件目录执行；替换所有绝对路径占位符。`--offline`是操作者已停止目标宿主的声明，不是自动进程检测。

```sh
node scripts/manage.mjs repair-replies --destination /ABS/COPY/what_we_did.json --offline
node scripts/manage.mjs repair-replies --destination /ABS/COPY/what_we_did.json --offline --execute --reason '修复旧版历史回放覆盖' --backup-dir /ABS/BACKUPS
```

可加`--record-id`限定单条。输出ID、旧新长度、原来源和未确定原因，不批量打印全文。重复相同修复零业务变化；原事件、无关字段、updatedAt保留，不续TTL；过期、删除、归档不会复活。符合原展示窗口的接收会话在下次正常请求可看到同id纠正delta。

## 导入旧任务插件did

关闭旧录制、工具和注入之后再启用独立插件。不要删旧任务、会话或记忆。

```sh
node scripts/manage.mjs migrate --source /ABS/STORAGE/task_ui_did.json --destination /ABS/STORAGE/what_we_did.json --backup-dir /ABS/BACKUPS --offline --legacy-disabled
```

保留原ID、时间、状态、摘录和来源；只有legacy_imported基线，不伪造旧更新过程。重复导入幂等，冲突拒绝；旧关联只保留legacy信息。

## 导出与回退

```sh
node scripts/manage.mjs export --destination /ABS/STORAGE/what_we_did.json --output /ABS/NEW_EXPORT.json --offline
node scripts/manage.mjs restore --manifest /ABS/BACKUPS/BATCH/manifest.json --entry state --destination /ABS/STORAGE/what_we_did.json --offline --new-plugin-disabled --execute
```

回退前停止宿主、导出新增记录并关闭新插件。restore校验备份SHA256，并保留被覆盖目标的副本；`--entry legacy`用于旧did文件。配置回退不能恢复已物理删除的数据；导出新增记录不等于自动合并回旧插件。旧版继续写入新域可能丢弃新增控制字段并重现缺陷，应优先使用修复后的版本。

## 保存策略维护

默认永久。有限策略必须明确archive/delete。配置缩短先形成待确认策略，物理清理再单独执行。执行前用`maintain`不带`--execute`预览；实际执行须另带`--execute --authorize-maintenance`及备份目录。归档不在普通查询中返回，可通过离线export或备份恢复；delete不可从现有正文直接找回。
