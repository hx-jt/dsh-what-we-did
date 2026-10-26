# 开发与测试

当前测试在已有DSH构建的工作区验证，不是无依赖的独立仓库测试。源码保留测试文件；宿主集成测试需要外部工作区，没有独立 CI。

现有布局要求：插件目录与deepseek-harness、dsh-global-task-list-main相邻。DSH需已构建；真实task联合测试需要纯任务增量版本。插件运行依赖和宿主peer依赖需可解析。test/host.mjs通过相邻DSH的包解析宿主模块；test/package.mjs用本地依赖验证解包加载。生产tgz不含node_modules和测试数据。

```sh
# 从插件目录执行；证据位置显式指定到独立批次。
WWD_EVIDENCE_DIR="$PWD/.artifacts/release-check" npm run check
```

该命令包括语法、规则、真实Cordis/Agent-loop/JSON域捕获测试，以及打包/解包加载。没有真实模型API调用。集成测试使用临时存储，不应指向运行中的用户账本。

请按上述相邻目录布局运行集成测试；独立克隆本仓库后，不能在缺少已构建宿主和任务插件时直接运行完整 check。
