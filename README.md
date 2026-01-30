# Jinfuzi Intern
![Author](https://img.shields.io/badge/Author-MKCorleonE-blue?logo=github
)
![LICENSE](https://img.shields.io/badge/license-MIT-green)

> This project is mainly used to document the work carried out during the internship period.

## Project structure
```
Jinfuzi-Intern/
├── data/
├── utils/
├── src/
├── logs/
├── LICENSE
├── README.md
└── requirement.txt
```
## Data Download
The data is sourced from the business interface. For the detailed data acquisition script, please refer to the "src" folder.

## Data Process
Merge multiple data into a single panel data file.

## Realtime trading strategy (currently in use...)
> The project has established a complete quantitative strategy backtesting framework, implemented an asset allocation strategy driven by dual factors (RTVR + TSM), and supports real-time recommendation output and performance analysis.

### Factor list
- RTVR 成交拥挤度因子
- TSM  交易情绪动量因子

### Realtime Object
- 华泰柏瑞中证500ETF 512510
- 红利ETF易方达 515180

### Signal Source
- 000905 中证500指数
- 000922 中证红利指数
- 000985 中证全指(暂停使用)

### RTVR 成交拥挤度因子
- 因子计算公式： 中证500指数交易额 / (中证500指数交易额 + 中证红利指数交易额)，取40天的滑动平均值  
- 策略触发条件： 计算当前因子值在过去66天中的分位数，当RTVR因子当日对应的历史分位数大于0.70（当前设定的阈值），卖出华泰柏瑞中证500ETF（进行调仓）
- 额外过滤条件： 回测初始仓位50%/50%，（连续三天在阈值上斜率大于0/连续三天在阈值下斜率小于0）每触发一次交易信号调仓，只增强或保持（根据超出幅度大的调仓），对于加仓方：持有仓位=50%+超出调仓阈值幅度/（满仓阈值-调仓阈值）*50%，减仓： w_500 = 0.50 - exceed_ratio * 0.50，
调仓阈值为 70%/30%，满仓阈值为90%/10%。 同时40—60%的分位数仓位调整至基准（各持仓50%），30-40%和60-70%的分位数仓位是保持不变的。
### TSM  交易情绪动量因子
- 因子构建：
  - 隔夜情绪OTSM：[（最高价-前收盘价）]/（最高价-最低价），取69天平滑平均值
  - 盘中情绪DTSM：[（最高价-开盘价）-（开盘价-最低价）]/（最高价-最低价）， 取3天平滑平均值
  - TSM=0.5$\times$OTSM+0.5$\times$DTSM
  - TSM相对=中证500指数的TSM-中证红利指数的TSM，取25天平滑平均值
- 调仓策略：
  - 1 (满仓 500),Factor>0.04,连续 3 天为正,且VR500>20日成交量均值，因子进入强多头区，且动量持续增强。  
2 (满仓 红利),Factor<−0.04,连续 3 天为负,且VRHL>20日成交量均值，因子进入强空头区，且动量持续增强。  
3 (回撤 50/50),Factor>0.04,连续 3 天为负,因子处于多头区，但动量衰竭/反转，主动平仓减风险。  
4 (回撤 50/50),Factor<−0.04,连续 3 天为正,因子处于空头区，但动量衰竭/反转，主动平仓减风险。
保持,−0.04≤Factor≤0.04,任意,因子处于中性区，不调仓，保持前一天的仓位。
  - 斜率调仓：今日步长 = MIN + (斜率绝对值 * SENSITIVITY)，min=1%，SENSITIVITY=30，斜率越大调仓速度越快。


## ⭐ Stars
[![Star History Chart](https://api.star-history.com/svg?repos=MKCorleonE/Jinfuzi-Intern&type=Date&theme=dark)](https://star-history.com/#MKCorleonE/G1VENQUANT&Date)