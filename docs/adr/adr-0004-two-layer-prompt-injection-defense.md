# ADR-0004: Prompt 注入双层防护（正则快路 + LLM 慢路）

- 状态：已接受
- 日期：2026-08-31

## 背景

用户输入直接拼进 LLM prompt，存在角色劫持、系统提示词套取、指令覆盖风险。
单层正则（`api/middleware/prompt_sanitizer.py` 快路）零成本但易被同义改写绕过；
单层 LLM 分类对每条输入做语义判定准确但延迟/算力开销大。

## 决策

采用双层漏斗（`sanitize_user_input_async` 为唯一管线入口）：

1. **快路（同步正则，零成本）**：危险关键词 + 角色劫持 + 数据泄露模式 +
   长度上限。命中且 `PROMPT_INJECTION_REJECT=True` → 直接拒绝（不再调 LLM）；
   默认告警模式 → 标记 violations 并用 `<user_input_begin/end>` 包裹放行。
2. **慢路（本地 Ollama 分类器）**：未被快路拒绝的输入交给 LLM 二判
   （`PROMPT_INJECTION_LLM_MODEL`，JSON 输出，置信度阈值 0.7）。
   - LLM 确认注入（confidence ≥ 阈值）→ **拒绝**（即使快路是告警模式）；
   - 疑似（< 阈值）→ 升级为告警；良性 → 维持原判定。
3. **降级策略**：LLM 不可用/解析失败默认 fail-open（放行 + 告警日志），
   可 `PROMPT_INJECTION_LLM_FAIL_CLOSED=1` 切换为拒绝。
4. **审计**：所有 warning/reject 事件落盘 JSONL（`SECURITY_AUDIT_LOG_PATH`），
   含 LLM verdict 全量字段，可回溯误杀/漏报。

## 后果

- 正面：快路漏报由慢路兜底，慢路故障不阻断业务（可用性优先可配置）；
  金融领域豁免写进分类器 prompt（"查询API数据"等正常表述不误杀）。
- 负面：每条 clean 输入多一次本地模型调用（~1-3s 延迟）；可通过
  `PROMPT_INJECTION_LLM_ENABLED=0` 关闭退回单层正则。

## 替代方案

- 纯 LLM 判定所有输入：延迟与算力成本不可接受，否决。
- 纯正则：同义改写（"把之前的设定扔掉"）零防御，否决。
