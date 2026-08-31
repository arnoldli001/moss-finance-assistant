# 架构决策记录（ADR）

本项目用 ADR 记录不可逆/高影响的技术决策。每条 ADR 遵循轻量模板：
状态 / 背景 / 决策 / 后果 / 替代方案。新决策追加编号，不修改已接受的旧决策。

| 编号 | 决策 | 状态 |
|------|------|------|
| [ADR-0001](adr-0001-layered-structure-compat-shims.md) | 新分层结构 + 兼容垫片（真源唯一） | 已接受 |
| [ADR-0002](adr-0002-constants-single-source.md) | 全局常量平铺真源 + 分组视图 | 已接受 |
| [ADR-0003](adr-0003-ptd-with-adaptive-gating.md) | PTD 渐进式工具披露 + 自适应门控 | 已接受 |
| [ADR-0004](adr-0004-two-layer-prompt-injection-defense.md) | Prompt 注入双层防护（正则快路 + LLM 慢路） | 已接受 |
| [ADR-0005](adr-0005-test-gating-and-ci.md) | 网络测试门控与 CI 策略 | 已接受 |
