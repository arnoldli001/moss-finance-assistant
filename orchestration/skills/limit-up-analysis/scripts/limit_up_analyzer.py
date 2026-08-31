"""
涨停股分析器核心脚本
Limit-Up Stock Analyzer

功能：
1. 封板强度五维打分（封板时间/封单额比/开板次数/换手率/板块联动）
2. 情绪周期四阶段判定（冰点/回暖/高潮/退潮）
3. 次日溢价率预测（基础溢价+修正因子）
4. 连板梯队梳理工具

使用方式：
    from skills.limit_up_analysis.scripts.limit_up_analyzer import (
        SealStrengthScorer,
        SentimentCycleJudge,
        NextDayPremiumPredictor,
        classify_board_type,
    )
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


# ======================================================================
# 枚举类型定义
# ======================================================================

class BoardType(str, Enum):
    """涨停板形态枚举（对应SKILL规则1 维度B）"""
    YIZI = "一字板"          # 开盘即封死，全天不开板
    T_SHAPED = "T字板"       # 开板后回封，收盘价=涨停价
    MIAO = "秒板"            # 开盘5分钟内封死
    MORNING_HARD = "早盘硬板"  # 10:00前封死
    MIDDAY_HARD = "午盘硬板"   # 10:00-14:00封死
    LATE_BOARD = "尾盘板"     # 14:30后封板
    LAN = "烂板"             # 开板≥3次或尾盘勉强回封
    ZHABAN = "炸板未回封"     # 收盘未封住


class SentimentPhase(str, Enum):
    """情绪周期四阶段（对应SKILL规则5）"""
    FROZEN = "冰点期"
    WARMING = "回暖期"
    CLIMAX = "高潮期"
    EBBING = "退潮期"


class SealStrengthRating(str, Enum):
    """封板强度评级（对应SKILL规则2 评分表）"""
    EXTREMELY_STRONG = "极强封板"   # ≥80分
    STRONG = "较强封板"             # 60-79分
    NORMAL = "一般封板"             # 40-59分
    WEAK = "较弱封板"               # <40分


# ======================================================================
# 规则1：涨停板类型分类
# ======================================================================

def classify_board_type(
    seal_time_minutes: int,        # 封板时间（从9:30开盘算起，单位分钟），None=未回封
    open_times: int,               # 开板次数
    turnover_rate: Optional[float] = None,  # 换手率（可选）
    open_price_equal_limit: bool = True,     # 开盘价是否=涨停价
) -> BoardType:
    """
    按规则1 维度B 判断涨停板形态。

    参数：
        seal_time_minutes: 最终封板时刻距开盘的分钟数。9:30=0, 10:00=30,
                           11:30=120, 13:00=150, 14:30=240, 15:00=270。
                           若收盘未封住（炸板），传 None。
        open_times: 盘中开板次数（0=未开板，1=开板1次回封，以此类推）
        turnover_rate: 换手率（可选，辅助判断缩量一字板）
        open_price_equal_limit: 开盘价是否直接等于涨停价（一字板前提）

    返回：BoardType 枚举值
    """
    # 炸板未回封优先判定
    if seal_time_minutes is None:
        return BoardType.ZHABAN

    # 一字板：开盘=涨停价 + 0次开板
    if open_price_equal_limit and open_times == 0:
        return BoardType.YIZI

    # T字板：开盘=涨停价 + 有开板但已回封
    if open_price_equal_limit and open_times >= 1:
        return BoardType.T_SHAPED

    # 秒板：5分钟内封板 + 开板≤1次
    if seal_time_minutes <= 5 and open_times <= 1:
        return BoardType.MIAO

    # 烂板：开板≥3次
    if open_times >= 3:
        return BoardType.LAN

    # 按封板时间段分
    if seal_time_minutes <= 30:       # 10:00前
        return BoardType.MORNING_HARD
    elif seal_time_minutes <= 210:    # 14:00前（注意11:30-13:00休市已包含在时间差计算）
        return BoardType.MIDDAY_HARD
    else:                              # 14:30后
        return BoardType.LATE_BOARD


# ======================================================================
# 规则2：封板强度五维打分
# ======================================================================

@dataclass
class SealStrengthResult:
    """封板强度评分结果"""
    seal_time_score: int = 0          # 维度1 封板时间 /20
    seal_order_score: int = 0         # 维度2 封单额比 /20
    open_times_score: int = 0         # 维度3 开板次数 /20
    turnover_score: int = 0           # 维度4 换手率 /20
    sector_linkage_score: int = 0     # 维度5 板块联动 /20

    @property
    def total_score(self) -> int:
        return (self.seal_time_score + self.seal_order_score
                + self.open_times_score + self.turnover_score
                + self.sector_linkage_score)

    @property
    def rating(self) -> SealStrengthRating:
        s = self.total_score
        if s >= 80:
            return SealStrengthRating.EXTREMELY_STRONG
        elif s >= 60:
            return SealStrengthRating.STRONG
        elif s >= 40:
            return SealStrengthRating.NORMAL
        else:
            return SealStrengthRating.WEAK

    def to_markdown(self) -> str:
        return (
            f"- 封板时间：{self.seal_time_score}/20 | "
            f"封单额比：{self.seal_order_score}/20 | "
            f"开板次数：{self.open_times_score}/20 | "
            f"换手率：{self.turnover_score}/20 | "
            f"板块联动：{self.sector_linkage_score}/20\n"
            f"- **综合得分：{self.total_score}分 → 评级：{self.rating.value}**"
        )


class SealStrengthScorer:
    """封板强度五维评分器（对应SKILL规则2）"""

    @staticmethod
    def _score_seal_time(seal_time_minutes: int) -> int:
        """维度1：封板时间打分（满分20）"""
        if seal_time_minutes <= 5:
            return 20
        elif seal_time_minutes <= 30:
            return 16
        elif seal_time_minutes <= 120:
            return 12
        elif seal_time_minutes <= 210:
            return 8
        elif seal_time_minutes <= 240:
            return 4
        else:
            return 0

    @staticmethod
    def _score_seal_order(seal_amount_yuan: float, float_market_cap_yuan: float) -> int:
        """
        维度2：封单额/流通市值比 打分（满分20）
        参数单位统一为「元」即可，只算比值。
        """
        if float_market_cap_yuan <= 0:
            return 0
        ratio = seal_amount_yuan / float_market_cap_yuan
        if ratio >= 0.01:        # ≥1%
            return 20
        elif ratio >= 0.005:     # 0.5%~1%
            return 16
        elif ratio >= 0.002:     # 0.2%~0.5%
            return 12
        elif ratio >= 0.001:     # 0.1%~0.2%
            return 8
        elif ratio >= 0.0005:    # 0.05%~0.1%
            return 4
        else:
            return 0

    @staticmethod
    def _score_open_times(open_times: int) -> int:
        """维度3：开板次数打分（满分20）"""
        if open_times == 0:
            return 20
        elif open_times == 1:
            return 15
        elif open_times == 2:
            return 10
        elif open_times == 3:
            return 5
        else:
            return 0

    @staticmethod
    def _score_turnover(turnover_rate_pct: float, is_lianban: bool) -> int:
        """
        维度4：换手率打分（满分20）
        turnover_rate_pct: 百分比数值，例如 12.5 表示 12.5%
        is_lianban: 是否是连板（非首板）
        """
        if is_lianban:
            # 连板股：10-20% 最佳
            if 10 <= turnover_rate_pct <= 20:
                return 20
            elif turnover_rate_pct < 5 or turnover_rate_pct > 40:
                return 12
            else:
                return 8
        else:
            # 首板：5-15% 最佳
            if 5 <= turnover_rate_pct <= 15:
                return 20
            elif turnover_rate_pct > 30:
                return 12
            elif turnover_rate_pct < 3:
                return 8
            else:
                return 8  # 15-30% 首板换手也算中规中矩

    @staticmethod
    def _score_sector_linkage(same_sector_limit_count: int) -> int:
        """维度5：板块联动打分（满分20）。同板块当日涨停家数。"""
        if same_sector_limit_count >= 5:
            return 20
        elif same_sector_limit_count >= 3:
            return 15
        elif same_sector_limit_count >= 1:
            return 8
        else:
            return 0

    def score(
        self,
        seal_time_minutes: Optional[int],
        seal_amount_yuan: float,
        float_market_cap_yuan: float,
        open_times: int,
        turnover_rate_pct: float,
        same_sector_limit_count: int,
        is_lianban: bool,
    ) -> SealStrengthResult:
        """
        五维综合打分。

        参数说明见各 _score_xxx 方法注释。
        seal_time_minutes=None 表示炸板，封板时间维度直接给 0 分。
        """
        result = SealStrengthResult()
        result.seal_time_score = self._score_seal_time(seal_time_minutes or 9999)
        result.seal_order_score = self._score_seal_order(seal_amount_yuan, float_market_cap_yuan)
        result.open_times_score = self._score_open_times(open_times)
        result.turnover_score = self._score_turnover(turnover_rate_pct, is_lianban)
        result.sector_linkage_score = self._score_sector_linkage(same_sector_limit_count)
        return result


# ======================================================================
# 规则5：情绪周期四阶段判定
# ======================================================================

@dataclass
class MarketSnapshot:
    """某交易日的市场情绪快照（用于情绪周期判定）"""
    limit_up_count: int              # 涨停家数（成功封板）
    limit_down_count: int            # 跌停家数
    zhaban_count: int                # 炸板家数（未回封）
    max_lianban_height: int          # 最高连板数（如 5 表示 5 连板）
    # 可选指标，提升判定精度
    avg_yield_prev_limit_up: Optional[float] = None  # 昨日涨停股今日平均收益率（%）
    sz_volume_yuan: Optional[float] = None           # 上证成交额（元）

    @property
    def seal_rate(self) -> float:
        """封板率 = 成功涨停 / (成功涨停 + 炸板)。范围 0~1。"""
        total = self.limit_up_count + self.zhaban_count
        return (self.limit_up_count / total) if total > 0 else 0.0

    @property
    def zhaban_rate(self) -> float:
        """炸板率 = 炸板 / (成功涨停 + 炸板)。范围 0~1。"""
        total = self.limit_up_count + self.zhaban_count
        return (self.zhaban_count / total) if total > 0 else 0.0


@dataclass
class SentimentJudgeResult:
    """情绪周期判定结果"""
    phase: SentimentPhase
    confidence: str                              # "高/中/低"
    evidence: List[str] = field(default_factory=list)  # 判定依据明细

    def to_markdown(self) -> str:
        lines = [
            f"- **情绪周期判定**：{self.phase.value}（置信度：{self.confidence}）",
            "- 判定依据："
        ]
        for i, e in enumerate(self.evidence, 1):
            lines.append(f"  {i}. {e}")
        return "\n".join(lines)


class SentimentCycleJudge:
    """
    情绪周期四阶段判定器（对应SKILL规则5）。

    逻辑：
    1. 先根据 4 个硬指标（涨停数/高度/封板率/跌停数）做基础投票
    2. 若有昨日涨停平均收益率 / 成交量等软指标，作为加权修正
    3. 返回判定结果 + 依据明细 + 置信度
    """

    def judge(self, snapshot: MarketSnapshot) -> SentimentJudgeResult:
        votes: Dict[SentimentPhase, int] = {
            SentimentPhase.FROZEN: 0,
            SentimentPhase.WARMING: 0,
            SentimentPhase.CLIMAX: 0,
            SentimentPhase.EBBING: 0,
        }
        evidence: List[str] = []

        # --- 硬指标1：涨停家数 ---
        c = snapshot.limit_up_count
        if c < 30:
            votes[SentimentPhase.FROZEN] += 2
            evidence.append(f"涨停家数仅 {c} 家（<30家，符合冰点特征）")
        elif c <= 60:
            votes[SentimentPhase.WARMING] += 2
            evidence.append(f"涨停家数 {c} 家（30-60家区间，符合回暖特征）")
        elif c > 80:
            votes[SentimentPhase.CLIMAX] += 2
            evidence.append(f"涨停家数 {c} 家（>80家，符合高潮特征）")
        else:
            # 60-80之间，可能是高潮→退潮过渡期
            votes[SentimentPhase.EBBING] += 1
            votes[SentimentPhase.CLIMAX] += 1
            evidence.append(f"涨停家数 {c} 家（60-80家过渡区间）")

        # --- 硬指标2：连板高度 ---
        h = snapshot.max_lianban_height
        if h <= 2:
            votes[SentimentPhase.FROZEN] += 2
            evidence.append(f"最高连板仅 {h} 板（≤2板，高度压缩）")
        elif h <= 4:
            votes[SentimentPhase.WARMING] += 2
            evidence.append(f"最高连板 {h} 板（3-4板，回暖期高度）")
        else:  # ≥5
            votes[SentimentPhase.CLIMAX] += 2
            evidence.append(f"最高连板 {h} 板（≥5板，高潮期特征）")

        # --- 硬指标3：封板率 + 炸板率 ---
        sr = snapshot.seal_rate
        zr = snapshot.zhaban_rate
        if sr < 0.60 or zr > 0.40:
            votes[SentimentPhase.FROZEN] += 1
            if votes.get(SentimentPhase.FROZEN, 0) < 2:
                votes[SentimentPhase.EBBING] += 2
            evidence.append(
                f"封板率 {sr*100:.0f}%，炸板率 {zr*100:.0f}%（封板率<60%或炸板率>40%，符合冰点/退潮特征）"
            )
        elif 0.60 <= sr <= 0.75 or 0.25 <= zr <= 0.40:
            votes[SentimentPhase.WARMING] += 1
            evidence.append(
                f"封板率 {sr*100:.0f}%，炸板率 {zr*100:.0f}%（回暖期正常区间）"
            )
        elif sr > 0.80 or zr < 0.20:
            votes[SentimentPhase.CLIMAX] += 1
            evidence.append(
                f"封板率 {sr*100:.0f}%，炸板率 {zr*100:.0f}%（>80%封板率，高潮特征）"
            )

        # --- 硬指标4：跌停家数 ---
        d = snapshot.limit_down_count
        if d > 15:
            votes[SentimentPhase.FROZEN] += 1
            votes[SentimentPhase.EBBING] += 1
            evidence.append(f"跌停家数 {d} 家（>15家，亏钱效应显著）")
        elif d < 5:
            votes[SentimentPhase.CLIMAX] += 1
            evidence.append(f"跌停家数仅 {d} 家（<5家，几乎无亏钱效应）")
        else:
            votes[SentimentPhase.WARMING] += 1
            evidence.append(f"跌停家数 {d} 家（5-15家，回暖期水平）")

        # --- 软指标修正：昨日涨停今日平均收益率 ---
        if snapshot.avg_yield_prev_limit_up is not None:
            y = snapshot.avg_yield_prev_limit_up
            if y < -2:
                votes[SentimentPhase.FROZEN] += 1
                evidence.append(f"昨日涨停今日均收益 {y:+.1f}%（<-2%，打板集体吃面）")
            elif y > 4:
                votes[SentimentPhase.CLIMAX] += 1
                evidence.append(f"昨日涨停今日均收益 {y:+.1f}%（>+4%，打板躺赚）")
            elif y > 1.5:
                votes[SentimentPhase.WARMING] += 1
                evidence.append(f"昨日涨停今日均收益 {y:+.1f}%（>+1.5%，回暖期赚钱效应）")
            else:
                votes[SentimentPhase.EBBING] += 1
                evidence.append(f"昨日涨停今日均收益 {y:+.1f}%（低迷，退潮特征）")

        # --- 取投票最多的阶段 ---
        sorted_phases = sorted(votes.items(), key=lambda x: -x[1])
        top_phase, top_votes = sorted_phases[0]
        second_votes = sorted_phases[1][1] if len(sorted_phases) > 1 else 0

        # 置信度：领先票数越多，置信度越高
        lead = top_votes - second_votes
        if lead >= 3:
            confidence = "高"
        elif lead >= 1:
            confidence = "中"
        else:
            confidence = "低"

        return SentimentJudgeResult(phase=top_phase, confidence=confidence, evidence=evidence)


# ======================================================================
# 规则4：次日溢价率预测
# ======================================================================

# 规则4：基础溢价率表（中位数值，供脚本内计算用；SKILL.md展示区间更合适）
_BASE_PREMIUM: Dict[BoardType, Tuple[float, float, float]] = {
    # BoardType -> (高开概率%, 溢价率下限%, 溢价率上限%)
    BoardType.YIZI:        (90.0,  5.0, 10.0),
    BoardType.T_SHAPED:    (75.0,  3.0,  6.0),
    BoardType.MIAO:        (70.0,  2.0,  5.0),
    BoardType.MORNING_HARD:(70.0,  2.0,  5.0),
    BoardType.MIDDAY_HARD: (55.0,  1.0,  3.0),
    BoardType.LATE_BOARD:  (40.0, -1.0,  1.0),
    BoardType.LAN:         (30.0, -3.0,  0.0),
    BoardType.ZHABAN:      (20.0, -5.0, -2.0),
}


@dataclass
class PremiumFactors:
    """修正因子开关（True表示命中该因子，False表示不命中）"""
    # 大盘修正
    market_rally_over_1pct: bool = False     # 大盘涨 > +1%
    market_drop_over_1pct: bool = False      # 大盘跌 > -1%
    # 板块修正
    belong_top3_sector: bool = False         # 属于当日最强TOP3板块
    # 龙头修正
    is_dragon_head: bool = False             # 是龙头股
    # 连续缩量一字板风险修正
    consecutive_yizi_over_3_low_turnover: bool = False  # 连续3+板缩量一字，换手<5%


@dataclass
class PremiumPrediction:
    """次日溢价率预测结果"""
    board_type: BoardType
    base_low_pct: float          # 基础溢价下限（%）
    base_high_pct: float         # 基础溢价上限（%）
    open_high_prob_pct: float    # 高开概率（%）
    factor_sum_pct: float        # 修正因子合计（%）
    final_low_pct: float         # 最终预期下限（%）
    final_high_pct: float        # 最终预期上限（%）
    warnings: List[str] = field(default_factory=list)  # 风险提示

    def to_markdown(self) -> str:
        lines = [
            f"- 封板类型：{self.board_type.value}",
            f"- 基础溢价率：{self.base_low_pct:+.1f}% ~ {self.base_high_pct:+.1f}%",
            f"- 高开概率：{self.open_high_prob_pct:.0f}%",
            f"- 修正因子合计：{self.factor_sum_pct:+.1f}%",
            f"- **最终预期溢价：{self.final_low_pct:+.1f}% ~ {self.final_high_pct:+.1f}%**",
        ]
        if self.warnings:
            lines.append("- ⚠️ 特别提示：")
            for w in self.warnings:
                lines.append(f"  - {w}")
        return "\n".join(lines)


class NextDayPremiumPredictor:
    """次日溢价率预测器（对应SKILL规则4）"""

    def predict(
        self,
        board_type: BoardType,
        seal_rating: SealStrengthRating,
        factors: PremiumFactors,
    ) -> PremiumPrediction:
        base_prob, base_low, base_high = _BASE_PREMIUM[board_type]

        # 封板强度在评分表范围内做微调（±1%）
        rating_bonus = {
            SealStrengthRating.EXTREMELY_STRONG: 1.0,
            SealStrengthRating.STRONG: 0.5,
            SealStrengthRating.NORMAL: 0.0,
            SealStrengthRating.WEAK: -0.5,
        }.get(seal_rating, 0.0)

        # 修正因子累加
        factor_sum = 0.0
        warnings: List[str] = []

        if factors.market_rally_over_1pct:
            factor_sum += 1.5
        if factors.market_drop_over_1pct:
            factor_sum -= 1.5
        if factors.belong_top3_sector:
            factor_sum += 2.0
        if factors.is_dragon_head:
            factor_sum += 2.5

        if factors.consecutive_yizi_over_3_low_turnover:
            factor_sum -= 3.0
            warnings.append(
                "连续3板以上缩量一字，今日若开板且换手>40%则大概率见顶，不建议接力"
            )

        # 加上强度微调
        factor_sum += rating_bonus

        final_low = round(base_low + factor_sum, 1)
        final_high = round(base_high + factor_sum, 1)

        # 弱封板+退潮期等极端情形，拉低下限更保守
        if seal_rating == SealStrengthRating.WEAK and board_type in (
            BoardType.LAN, BoardType.LATE_BOARD, BoardType.ZHABAN
        ):
            final_low -= 1.0
            final_high -= 0.5

        return PremiumPrediction(
            board_type=board_type,
            base_low_pct=base_low,
            base_high_pct=base_high,
            open_high_prob_pct=base_prob,
            factor_sum_pct=round(factor_sum, 1),
            final_low_pct=final_low,
            final_high_pct=final_high,
            warnings=warnings,
        )


# ======================================================================
# 连板梯队梳理（工具函数）
# ======================================================================

@dataclass
class LimitUpStock:
    """一只涨停股的核心字段（用于梯队梳理）"""
    name: str
    code: str
    lianban_height: int                # 连板数，1=首板，2=二板，以此类推
    board_type: BoardType
    seal_score_total: int              # 封板强度总分（0-100）
    concepts: List[str]                # 概念标签
    reason: str = ""                   # 涨停原因
    is_dragon_head_candidate: bool = False  # 是否是龙头候选

    def short_markdown(self) -> str:
        rating = SealStrengthScorer()  # 仅用于借用 rating 映射
        # 单独根据总分算评级
        if self.seal_score_total >= 80:
            r = SealStrengthRating.EXTREMELY_STRONG.value
        elif self.seal_score_total >= 60:
            r = SealStrengthRating.STRONG.value
        elif self.seal_score_total >= 40:
            r = SealStrengthRating.NORMAL.value
        else:
            r = SealStrengthRating.WEAK.value
        tag = " 🥇龙头候选" if self.is_dragon_head_candidate else ""
        return (
            f"- **{self.name}（{self.code}）**{tag} | "
            f"涨停类型：{self.board_type.value} | "
            f"封板强度：{self.seal_score_total}分（{r}）\n"
            f"  - 概念：{'、'.join(self.concepts) if self.concepts else '待归类'}"
            + (f"\n  - 涨停原因：{self.reason}" if self.reason else "")
        )


def build_lianban_ladder(stocks: List[LimitUpStock]) -> str:
    """
    按连板高度从高到低构建梯队 Markdown。
    自动在每个梯队里按封板强度降序排序，最高强度的自动标为龙头候选。
    """
    if not stocks:
        return "（无涨停股数据）"

    # 按高度分组
    by_height: Dict[int, List[LimitUpStock]] = {}
    for s in stocks:
        by_height.setdefault(s.lianban_height, []).append(s)

    # 每组内部按封板强度降序，第一名标记为龙头候选
    for h, grp in by_height.items():
        grp.sort(key=lambda x: -x.seal_score_total)
        if grp:
            grp[0].is_dragon_head_candidate = True

    # 从高到低输出
    lines: List[str] = []
    for h in sorted(by_height.keys(), reverse=True):
        if h == 1:
            lines.append(f"\n【首板】（{len(by_height[h])}家）")
        else:
            lines.append(f"\n【{h}连板】（{len(by_height[h])}家）")
        for s in by_height[h]:
            lines.append(s.short_markdown())

    return "\n".join(lines)


# ======================================================================
# 快速演示（python skills/limit-up-analysis/scripts/limit_up_analyzer.py）
# ======================================================================

def _demo():
    print("=" * 60)
    print("涨停股分析器 快速演示")
    print("=" * 60)

    # 1) 涨停板类型分类
    print("\n【1】涨停板类型分类示例：")
    samples = [
        ("一字板（无开板）", 0, 0, True),
        ("T字板（开板1次回封）", 100, 1, True),
        ("秒板（2分钟封板）", 2, 0, False),
        ("午盘板（14:00封）", 210, 1, False),
        ("尾盘板（14:45封）", 255, 0, False),
        ("烂板（开板4次）", 180, 4, False),
        ("炸板未回封", None, 3, False),
    ]
    for label, t, opens, is_limit_open in samples:
        bt = classify_board_type(t, opens, open_price_equal_limit=is_limit_open)
        print(f"  - {label:30s} → {bt.value}")

    # 2) 封板强度打分
    print("\n【2】封板强度打分示例（某早盘二板龙头）：")
    scorer = SealStrengthScorer()
    r = scorer.score(
        seal_time_minutes=8,      # 9:38封板
        seal_amount_yuan=5_000_000_000,   # 封单5亿
        float_market_cap_yuan=200_000_000_000,  # 流通市值200亿
        open_times=0,
        turnover_rate_pct=12.5,
        same_sector_limit_count=7,
        is_lianban=True,
    )
    print("  " + r.to_markdown().replace("\n", "\n  "))

    # 3) 情绪周期判定
    print("\n【3】情绪周期判定示例（高潮期）：")
    snap = MarketSnapshot(
        limit_up_count=112,
        limit_down_count=3,
        zhaban_count=15,
        max_lianban_height=7,
        avg_yield_prev_limit_up=4.8,
    )
    judge = SentimentCycleJudge()
    jr = judge.judge(snap)
    print("  " + jr.to_markdown().replace("\n", "\n  "))

    # 4) 次日溢价率预测
    print("\n【4】次日溢价率预测示例（高潮期T字板龙头）：")
    predictor = NextDayPremiumPredictor()
    pred = predictor.predict(
        board_type=BoardType.T_SHAPED,
        seal_rating=SealStrengthRating.EXTREMELY_STRONG,
        factors=PremiumFactors(
            market_rally_over_1pct=True,
            belong_top3_sector=True,
            is_dragon_head=True,
        ),
    )
    print("  " + pred.to_markdown().replace("\n", "\n  "))

    # 5) 连板梯队示例
    print("\n【5】连板梯队示例：")
    demo_stocks = [
        LimitUpStock("九安医疗", "002432", 5, BoardType.YIZI, 95,
                     ["新冠检测", "医疗器械"], "海外订单超预期"),
        LimitUpStock("浙江建投", "002761", 5, BoardType.T_SHAPED, 88,
                     ["基建", "浙江"], "基建政策利好"),
        LimitUpStock("中国医药", "600056", 3, BoardType.MORNING_HARD, 82,
                     ["医药商业", "新冠药代理"], "辉瑞协议"),
        LimitUpStock("天保基建", "000965", 3, BoardType.MIAO, 90,
                     ["房地产", "天津"], "地产松绑"),
        LimitUpStock("阳光城", "000671", 2, BoardType.MIDDAY_HARD, 68,
                     ["房地产"], "债务重组预期"),
        LimitUpStock("海泰发展", "600082", 1, BoardType.MORNING_HARD, 72,
                     ["房地产", "天津"], "板块补涨"),
    ]
    print("  " + build_lianban_ladder(demo_stocks).replace("\n", "\n  "))


if __name__ == "__main__":
    _demo()
