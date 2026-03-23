# AUSUB 定投策略指南

本文档说明 AUSUB 系统的完整买卖逻辑。当你从排行榜获取到一组最优因子参数后，可以按照以下规则制定自己的 AU9999 定投计划。

---

## 一、核心概念

系统使用三个技术指标辅助决策：

| 指标 | 计算方式 | 用途 |
|------|---------|------|
| MA(N) | 最近 N 日收盘价的简单移动平均 | 判断价格趋势，触发追加买入 |
| RSI(14) | 14 日相对强弱指标 | 判断超买超卖，辅助市场分类和卖出 |
| 布林带上轨 | MA20 + 2 × 标准差(20) | 辅助第二档卖出判断 |

---

## 二、市场状态分类

每个交易日，系统根据当前价格计算以下信号，满足其中 **≥ 2 项** 即归入对应状态：

### 极弱（oversold）
- 收盘价低于 MA20 超过 `discountToMa20` 阈值（如 -3%）
- RSI ≤ `oversoldRsi`（如 35）
- 近 60 日最大回撤 ≥ `deepDrawdown`（如 8%）
- 收盘价低于 MA60

### 偏弱（weak）
- 收盘价低于 MA20
- RSI 在 `oversoldRsi` ~ `weakRsiMax` 之间（如 35~45）
- 近 60 日回撤在 `drawdownWeak` ~ `deepDrawdown` 之间（如 4%~8%）

### 偏热（hot）
- 收盘价高于 MA20 超过 `premiumToMa20` 阈值（如 5%）
- RSI ≥ `hotRsi`（如 68）
- 收盘价高于 MA60
- 近 60 日回撤 < `drawdownWeak`

### 中性（neutral）
- 不满足以上任何分类

---

## 三、买入策略

### 3.1 定期定投（核心）

按固定周期自动买入，有两种定投模式：

| 参数 | 说明 |
|------|------|
| `scheduleMode` | `every_n_days`：每 N 个交易日买入一次；`weekly_weekday`：每周固定星期几买入 |
| `scheduleDays` | 定投间隔天数（仅 every_n_days 模式） |
| `scheduleWeekday` | 定投星期几，1=周一 ... 5=周五（仅 weekly_weekday 模式） |
| `baseAmount` | 每次定投的基础金额（元） |

**动态定投**（`dynamicEnabled = true` 时生效）：

定投金额不再固定，而是根据当前市场状态乘以对应倍率：

| 市场状态 | 倍率参数 | 示例 | 实际金额（基础 500 元） |
|---------|---------|------|----------------------|
| 极弱 | `oversoldMultiplier` | 1.6 | 800 元 |
| 偏弱 | `weakMultiplier` | 1.2 | 600 元 |
| 中性 | `neutralMultiplier` | 1.0 | 500 元 |
| 偏热 | `hotMultiplier` | 0.7 | 350 元 |

> 简单说：市场越弱买越多，市场越热买越少。

### 3.2 跌破均线追加

当价格跌破 MA(N) 均线时，额外追加买入 `dipAmount` 金额：

- **首次跌破**：价格从均线上方跌到下方的第一天，追加一笔
- **持续追加**：如果价格仍在均线下方，且当日收盘价比前一日更低，继续追加
- 价格回到均线上方后，追加机制重置

### 3.3 买入方式

| `buyMode` | 行为 |
|-----------|------|
| `intraday_break_same_close` | 盘中跌破均线 → 当日收盘价成交 |
| `close_confirm_next_close` | 收盘确认跌破 → 次日收盘价成交 |

---

## 四、卖出策略

系统设置三档阶梯式止盈，每档独立判断，可同时触发。所有卖出均在**次日收盘价**成交。

### 第一档：温和止盈
同时满足以下条件时，卖出持仓的 `sell1Pct` 比例：
- 收盘价高于 MA20 达到 `sell1Premium`（如 10%）
- RSI ≥ `sell1Rsi`（如 70）

### 第二档：布林带止盈
同时满足以下条件时，卖出持仓的 `sell2Pct` 比例：
- 收盘价高于 MA20 达到 `sell2Premium`（如 12%）
- 收盘价 ≥ 布林带上轨 ×（1 + `sell2BollBuffer`）

### 第三档：强势止盈
同时满足以下条件时，卖出持仓的 `sell3Pct` 比例：
- 收盘价高于 MA20 达到 `sell3Premium`（如 15%）
- RSI ≥ `sell3Rsi`（如 75）

> 三档从低到高递进：溢价阈值逐档升高，卖出比例也逐档加大。市场越疯狂，卖得越多。

---

## 五、完整参数表

以下是一组参数示例，你可以从排行榜获取实际最优值：

```
基础定投金额 (baseAmount):        500
追加金额 (dipAmount):             100
定投模式 (scheduleMode):          every_n_days
定投周期 (scheduleDays):          20
定投星期 (scheduleWeekday):       3（周三）
买入方式 (buyMode):               close_confirm_next_close
均线窗口 (maWindow):              20

动态定投 (dynamicEnabled):        true
极弱倍率 (oversoldMultiplier):    1.6
偏弱倍率 (weakMultiplier):        1.2
中性倍率 (neutralMultiplier):     1.0
偏热倍率 (hotMultiplier):         0.7

超卖 RSI (oversoldRsi):           35
偏弱 RSI 上限 (weakRsiMax):       45
偏热 RSI (hotRsi):                68
低于 MA20 阈值 (discountToMa20):  -3%
高于 MA20 阈值 (premiumToMa20):   5%
偏弱回撤 (drawdownWeak):          4%
深度回撤 (deepDrawdown):          8%

第一档卖出溢价 (sell1Premium):    10%
第一档卖出 RSI (sell1Rsi):        70
第一档卖出比例 (sell1Pct):        10%
第二档卖出溢价 (sell2Premium):    12%
第二档卖出比例 (sell2Pct):        15%
第二档布林缓冲 (sell2BollBuffer): 0%
第三档卖出溢价 (sell3Premium):    15%
第三档卖出 RSI (sell3Rsi):        75
第三档卖出比例 (sell3Pct):        20%
```

---

## 六、实操流程

拿到排行榜的最优参数后，按以下步骤执行：

1. **确定定投周期**：根据 `scheduleMode` 和 `scheduleDays`/`scheduleWeekday`，在日历上标记每个定投日
2. **每个定投日**：
   - 查看当日 RSI、MA20、MA60、近 60 日最高价，判断市场状态
   - 用 `baseAmount × 对应倍率` 计算本次定投金额
   - 按 `buyMode` 决定成交时机
3. **每个交易日检查追加**：
   - 如果价格跌破 MA(N)，追加 `dipAmount`
   - 如果已在均线下方且继续下跌，持续追加
4. **每个交易日检查卖出**：
   - 计算收盘价相对 MA20 的溢价百分比
   - 依次检查三档卖出条件，满足则次日卖出对应比例

---

## 七、评分公式

排行榜的评分方式：

```
评分 = 收益率(%) - 交易次数 × 惩罚权重
```

惩罚权重默认 0.08，意味着每多一笔交易扣 0.08 分。这鼓励系统找到**高收益且低频交易**的策略组合。

---

## 八、注意事项

- 回测结果基于历史数据，不代表未来收益
- 参数在不同时间段的表现可能差异很大，建议关注多个时间段都排名靠前的参数组合
- 实际操作中需考虑交易手续费、滑点等回测未计入的成本
- 建议先用小资金验证，确认策略符合预期后再加大投入
