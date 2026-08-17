"""
Layer 4 - Loop Engineering: 通用 SKILL 自动加载管理器。

每个 SKILL 是 skills/<skill_name>/ 下的一个目录，至少包含 SKILL.md。
SkillManager 负责：
1. 启动时扫描 skills/ 目录，注册每个 skill 的触发关键词
2. 在用户提问进入 main_agent 时，根据 query 关键词匹配应加载的 skills
3. 把匹配到的 SKILL.md 内容注入到 final_user_content 的前缀，指导 LLM 生成

触发关键词来源（优先级递减）：
  A. SKILL.md 顶部 YAML Frontmatter 的 `trigger-keywords:` 列表（推荐）
  B. skills/<name>/ 目录名的中英文拆分，含 name 本身
  C. SKILL.md 正文中首个二级标题 `## xxx` 的关键词（兜底）

典型注入场景：
  - 用户提问"交易系统故障处理" → 自动加载 trading-reliability/SKILL.md
  - 用户提问"生成一个下单重试机制的代码" → 自动加载 trading-reliability
  - 用户提问"复盘预测" → 自动加载 index-prediction（同时兼容 server.py 的显式加载）
"""
from __future__ import annotations

import re
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ----------------------------------------------------------------------
# Skill 注册数据结构
# ----------------------------------------------------------------------

@dataclass
class SkillDef:
    """一个 SKILL 的注册条目。"""
    name: str                      # 目录名，如 "trading-reliability"
    skill_dir: Path                # skills/<name>/ 目录
    skill_md_path: Path            # SKILL.md 绝对路径
    trigger_keywords: List[str]    # 触发关键词（小写，去重）
    description: str = ""          # YAML 中的 description（展示用）
    _content_cache: Optional[str] = field(default=None, repr=False)

    def load_content(self) -> str:
        """读取 SKILL.md 原文，缓存一次。"""
        if self._content_cache is None:
            try:
                self._content_cache = self.skill_md_path.read_text(encoding="utf-8")
            except Exception as e:
                self._content_cache = f"[SKILL 加载失败 {self.name}: {e}]"
        return self._content_cache


# ----------------------------------------------------------------------
# SkillManager 单例
# ----------------------------------------------------------------------

class SkillManager:
    """扫描 skills/ 并按关键词匹配用户查询。"""

    def __init__(self, skills_root: Path):
        self.skills_root = skills_root
        self._skills: Dict[str, SkillDef] = {}
        self._scan_and_register()

    # --------------------------------------------------------------
    # 扫描 & 注册
    # --------------------------------------------------------------
    def _scan_and_register(self) -> None:
        if not self.skills_root.exists():
            print(f"[SkillManager] skills 目录不存在: {self.skills_root}")
            return
        for subdir in sorted(self.skills_root.iterdir()):
            if not subdir.is_dir():
                continue
            md_path = subdir / "SKILL.md"
            if not md_path.exists():
                continue
            try:
                self._register_one(subdir, md_path)
            except Exception as e:
                print(f"[SkillManager] 注册 skill {subdir.name} 失败: {e}")

    def _register_one(self, skill_dir: Path, md_path: Path) -> None:
        name = skill_dir.name
        raw_md = md_path.read_text(encoding="utf-8")
        frontmatter, _ = _split_frontmatter(raw_md)
        keywords: List[str] = []

        # A. Frontmatter 中的 trigger-keywords（推荐方式）
        if frontmatter and isinstance(frontmatter, dict):
            fmk = frontmatter.get("trigger-keywords") or frontmatter.get("trigger_keywords")
            if isinstance(fmk, list):
                keywords.extend(str(k) for k in fmk if k)
            desc = frontmatter.get("description")
            if isinstance(desc, str):
                description = desc
            else:
                description = ""
        else:
            description = ""

        # B. 目录名本身 + 常见变体（去掉连字符/下划线再拆分中英文）
        keywords.append(name)
        for part in re.split(r"[-_\s]+", name):
            if part:
                keywords.append(part)

        # C. 正文第一个二级标题 ## xxx 的关键词
        m = re.search(r"^##\s+(.+)$", raw_md, re.MULTILINE)
        if m:
            title = m.group(1).strip()
            keywords.append(title)
            # 把中文标题的每段 2 字组合也加入（兜底粗匹配）
            chinese_chars = re.sub(r"[^\u4e00-\u9fff]", "", title)
            if len(chinese_chars) >= 2:
                keywords.append(chinese_chars)

        # 全部小写 + 去重 + 去空
        normalized: List[str] = []
        seen = set()
        for kw in keywords:
            k = kw.strip().lower()
            if not k or len(k) < 2:
                continue
            if k in seen:
                continue
            seen.add(k)
            normalized.append(k)

        sd = SkillDef(
            name=name,
            skill_dir=skill_dir,
            skill_md_path=md_path,
            trigger_keywords=normalized,
            description=description,
        )
        self._skills[name] = sd
        print(f"[SkillManager] 已注册 skill: {name} ({len(normalized)} 个触发关键词)")

    # --------------------------------------------------------------
    # 查询：匹配 skill
    # --------------------------------------------------------------
    def match_skills(self, user_query: str) -> List[SkillDef]:
        """根据用户提问文本返回匹配的 SkillDef 列表（按匹配关键词数量降序）。"""
        if not user_query:
            return []
        q = user_query.lower()
        scored: List[tuple] = []
        for sd in self._skills.values():
            hits = sum(1 for kw in sd.trigger_keywords if kw in q)
            if hits > 0:
                scored.append((hits, sd))
        # 命中越多越靠前；同命中则按技能名排序保证稳定
        scored.sort(key=lambda x: (-x[0], x[1].name))
        return [sd for _, sd in scored]

    def build_skill_prefix(self, user_query: str, max_skills: int = 2) -> str:
        """
        将匹配到的 SKILL.md 内容拼成注入到 user query 前的前缀字符串。
        匹配不到返回空串。
        为防止 context 爆炸，默认最多注入 2 个 skill（按匹配度排序取前 2）。
        """
        matched = self.match_skills(user_query)[:max_skills]
        if not matched:
            return ""
        blocks: List[str] = []
        for sd in matched:
            content = sd.load_content()
            blocks.append(
                f"\n{'='*6} 自动加载 Skill: {sd.name} {'='*6}\n"
                f"{content}\n"
                f"{'='*10} Skill 结束: {sd.name} {'='*10}\n"
            )
        return "\n".join(blocks)

    # --------------------------------------------------------------
    # 调试/健康检查
    # --------------------------------------------------------------
    def list_skills(self) -> Dict[str, Dict]:
        """返回所有注册技能的快照，供监控/Debug 使用。"""
        return {
            name: {
                "description": sd.description,
                "trigger_keywords": list(sd.trigger_keywords),
                "has_skill_md": sd.skill_md_path.exists(),
            }
            for name, sd in self._skills.items()
        }


# ----------------------------------------------------------------------
# YAML Frontmatter 解析
# ----------------------------------------------------------------------

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _split_frontmatter(md_text: str):
    """返回 (frontmatter_dict 或 None, 剩余正文)。"""
    m = _FM_RE.match(md_text)
    if not m:
        return None, md_text
    yaml_text = m.group(1)
    try:
        fm = yaml.safe_load(yaml_text)
        if not isinstance(fm, dict):
            fm = None
    except Exception:
        fm = None
    rest = md_text[m.end():]
    return fm, rest


# ----------------------------------------------------------------------
# 全局单例
# ----------------------------------------------------------------------

_skill_manager: Optional[SkillManager] = None


def get_skill_manager() -> SkillManager:
    """获取 SkillManager 全局单例（延迟初始化，首次调用触发 skills/ 扫描）。"""
    global _skill_manager
    if _skill_manager is None:
        project_root = Path(__file__).resolve().parents[1]
        skills_root = project_root / "skills"
        _skill_manager = SkillManager(skills_root)
    return _skill_manager
