---
name: dify-water-info-extract
description: "调用“智水大师信息萃取” Dify workflow API：上传本地转写文档和图片，以 `raw_scripts`、`photos` 和可选 `filename` 作为输入运行该工作流，并返回 Dify 响应；当前导出版本的主结果位于 `data.outputs.full_info_with_image_des`。Use when Codex needs to process local files with this specific 智水大师 workflow, or when debugging its Dify API invocation and output shape."
---

# Dify Water Info Extract

## Quick start
- 把这个 skill 视为“智水大师信息萃取”专用 skill，不要泛化到其他 Dify app。
- 优先用 `scripts/run_workflow.py`，不要手写 multipart 上传和工作流请求，除非需要临时排查底层 HTTP。
- 如果需要确认环境变量或 API 形状，先读 `references/env.md` 和 `references/api.md`。
- 如果需要确认这个 workflow 当前接受哪些输入，读 `references/workflow-summary.md`。
- 当前仓库里的最新导出文件是 `智水大师信息萃取 (1).yml`；修改脚本前优先对齐这份导出。
- 当前统一口径：
  - 脚本默认返回 Dify 的完整 JSON 响应
  - 传 `--print-outputs-only` 时只返回 `data.outputs`
  - 当前导出版本的主结果位于 `data.outputs.full_info_with_image_des`

## Run the workflow
- 设置环境变量：
  - `DIFY_WORKFLOW_API_BASE_URL`，值应包含 `/v1`，例如 `https://thuenv.tiangong.world:7001/v1`
  - `DIFY_WORKFLOW_API_KEY`
  - 可选：`DIFY_WORKFLOW_USER`
- 如果不想污染全局环境变量，把 app 专用配置写到 skill 目录下的 `.env.workflow.local`。
  - 先参考 `.env.workflow.local.example`
  - 脚本会自动读取 `.env.workflow.local`
  - 也可以显式传 `--env-file /path/to/file`
- 运行脚本时，用 `--raw-script` 传本地转写/文档文件，用 `--photo` 传现场图片。两个参数都支持重复传入。
- 如果用户已经给了最终结果文件名，同时传 `--filename`，这样会把该值一并送进 workflow 的 `inputs.filename`。
- 默认走阻塞式调用并打印完整 JSON 响应；如果只需要当前 workflow 的输出对象，用 `--print-outputs-only`。
- 默认已经带有更稳妥的 HTTP 参数：
  - `--upload-retries 3`
  - `--workflow-retries 0`
  - `--retry-delay-seconds 3`
  - `--connect-timeout-seconds 30`
  - `--request-timeout-seconds 1800`
- `--request-timeout-seconds` 现在默认是 30 分钟；超过这个阈值会被判定为超时。
- 上传前会默认校验本地文件：
  - 扩展名
  - MIME 推断
  - 文件头/内容签名
  - 文件大小（默认 500 MB）
- 如确需跳过本地校验，可显式传 `--skip-file-validation`，但不建议作为默认做法。
- 对真实文件，优先同时传：
  - `--output-file /tmp/xxx.json`
  - `--state-file /tmp/xxx.state.json`
- 这样即使上层会话中断，完整响应和中间状态也会留在本地。

```bash
python3 scripts/run_workflow.py \
  --raw-script /path/to/transcript.docx \
  --raw-script /path/to/notes.pdf \
  --filename 临沂调研记录 \
  --photo /path/to/site-1.jpg \
  --photo /path/to/site-2.png
```

## Merge extra inputs carefully
- 该 workflow 最新导出文件里，开始节点包含：
  - 文件型输入：`raw_scripts`、`photos`
  - 文本输入：`filename`
- 如果后续 Dify 侧新增了文本或开关参数，可把额外字段写进 `--inputs-json` 指向的 JSON 文件。
- 当 `--inputs-json` 里已经有 `raw_scripts` 或 `photos` 时，脚本会把本次上传得到的文件对象追加进去，而不是覆盖原值。
- 当同时提供 `--filename` 与 `--inputs-json` 时，CLI 传入的 `--filename` 会写入 `inputs.filename`。
- 如果工作流结构已经变更，先更新 `references/workflow-summary.md`，再调整脚本。
- 当前最新导出显示开始节点包含 `raw_scripts`、`photos`、`filename`，结束节点只暴露 `full_info_with_image_des` 这一项主结果；不要继续按旧版多结果配置处理。

## Interpret the response
- Dify 阻塞式 workflow 响应通常把 workflow 结果放在 `data.outputs`；若用户只要当前 workflow 的输出对象，优先返回该字段。
- 如果需要保留运行元数据、状态或调试信息，返回完整 JSON 响应。
- 当前导出版本里，主结果位于 `data.outputs.full_info_with_image_des`，其值通常是一个文件数组。
- 认证失败时，先核对 `Authorization: Bearer <key>`、API base URL 是否包含 `/v1`、以及该 key 是否属于目标 workflow app。
- 如果文件上传成功但 workflow 报输入错误，先检查 `raw_scripts` 是否为文档文件、`photos` 是否为图片文件。
- 如果报 `Could not resolve host`，通常是当前执行环境本身不允许外网访问，不是 skill 逻辑错误。
- 如果返回空对象或空输出，先确认输入文件是否真实有效；伪造扩展名但内容无效的测试图片/文档常会导致 workflow 无法抽取到结果。
- 如果文件在上传前就被脚本拒绝，先检查它是否是伪装扩展名、损坏文件，或超出大小阈值。
- 如果超时，优先查看 `--state-file`：
  - 若上传阶段已完成，说明问题多半出在服务端处理时间或 workflow 复杂度
  - 默认 `--workflow-retries 0`，避免因为客户端超时而重复触发同一条 workflow

## Resources
- `scripts/run_workflow.py`: 上传本地文件并调用 workflow API。
- `references/env.md`: 环境变量和推荐默认值。
- `references/api.md`: Dify 文件上传与 workflow 运行接口摘要。
- `references/workflow-summary.md`: 从仓库里的 workflow 导出文件提炼出的输入约定。
