# ADR-0002: 全局常量平铺真源 + 分组视图

- 状态：已接受
- 日期：2026-08-20（本记录 2026-08-31 补录）

## 背景

代码中散落大量魔鬼数字（180/300/2000/10 等），修改一处需全局 grep，易漏改。
曾尝试把 `shared/config/constants.py` 改成纯 re-export，导致 `TIMEOUTS` 分组
字典缺失、下游 ImportError。

## 决策

1. `config/constants.py` 是**平铺定义的唯一真源**：所有常量在此定义（含中文注释），
   支持环境变量覆盖的常量在此读 `os.getenv`。
2. `shared/config/constants.py` 只做 **re-export + 分组视图**（TIMEOUTS / SLO_TARGETS
   等字典），分组键值必须与 `orchestration/loop.py` 默认值对齐；平铺新增常量自动可见。
3. 使用方统一 `from config.constants import XXX`。

## 后果

- 正面：改一处全局生效；分组视图让编排层代码可读。
- 负面：两个文件容易被误当成"重复副本"去重——已在 AGENTS.md 中显式警示。

## 替代方案

- 全部分组字典化（无平铺）：调用方写 `C.TIMEOUTS.AGENT`，链式取值易打错且 diff 噪声大，否决。
