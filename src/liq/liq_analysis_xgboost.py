# ----------------------------------------------------------------------
# 机器学习增强版：特征工程 + XGBoost信号生成 + Sigmoid仓位管理
# ----------------------------------------------------------------------
import numpy as np
import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
import warnings
import os
warnings.filterwarnings('ignore')

# ----------------------------------------------------------------------
# 0. 全局设置
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 文件读取路径
FILE_PATH = './data/realtime_data_updated.csv'

# 回测时间段
BACKTEST_START_DATE = '2023-06-17'
BACKTEST_END_DATE = '2099-12-31'

# 策略起始日期
STRATEGY_START_DATE = '2026-01-25'

# 策略参数
WINDOW_SIZE = 40 # 计算信号的滚动窗口
HISTORY_WINDOW = 20  # 历史分位数窗口

# 交易成本与滑点
COST = 0.0002
SLIPPAGE = 0.0003

# ======================
# 1. 增强版特征工程（彻底解决StringDtype问题）
# ======================
def create_features(df):
    """
    三重保险机制：
    1. 使用select_dtypes安全筛选数值列（兼容pandas 2.0+）
    2. 显式验证必要列存在性
    3. 严格特征列筛选
    """
    df_feat = df.copy()
    
    # ===== 保险1：安全筛选数值列（兼容所有pandas版本）=====
    # 方法1：优先使用select_dtypes（最安全）
    try:
        df_feat = df_feat.select_dtypes(include=[np.number, 'number'])
    except Exception as e:
        print(f"⚠️ select_dtypes警告: {e}，尝试备用方案...")
        # 方法2：备用方案（遍历列检查）
        numeric_cols = []
        for col in df_feat.columns:
            if pd.api.types.is_numeric_dtype(df_feat[col]):
                numeric_cols.append(col)
        df_feat = df_feat[numeric_cols]
    
    # ===== 验证必要列存在 =====
    required_cols = ['Close_500', 'Close_HL', 'Ret_Index_500', 'Ret_Index_HL']
    missing_cols = [col for col in required_cols if col not in df_feat.columns]
    if missing_cols:
        raise ValueError(
            f"❌ 缺少必要数值列: {missing_cols}\n"
            f"   可用数值列示例: {df_feat.columns[:5].tolist()}\n"
            f"   请检查原始数据列名是否匹配（注意大小写/下划线）"
        )
    
    # ===== 生成特征（所有操作在纯数值df上）=====
    # 动量特征
    for window in [5, 10, 20, 60]:
        df_feat[f'Mom_500_{window}'] = df_feat['Close_500'].pct_change(window).shift(1)
        df_feat[f'Mom_HL_{window}'] = df_feat['Close_HL'].pct_change(window).shift(1)
        df_feat[f'Vol_500_{window}'] = df_feat['Ret_Index_500'].rolling(window).std().shift(1)
        df_feat[f'Vol_HL_{window}'] = df_feat['Ret_Index_HL'].rolling(window).std().shift(1)
    
    # 流动性特征（安全检查）
    if 'Volume_500' in df_feat.columns and 'Turnover_500' in df_feat.columns:
        ratio_500 = (df_feat['Turnover_500'] / df_feat['Volume_500']).replace([np.inf, -np.inf], np.nan)
        ratio_HL = (df_feat['Turnover_HL'] / df_feat['Volume_HL']).replace([np.inf, -np.inf], np.nan)
        df_feat['Turnover_Ratio_500'] = ratio_500
        df_feat['Turnover_Ratio_HL'] = ratio_HL
        for window in [5, 20]:
            df_feat[f'Turnover_MA_500_{window}'] = ratio_500.rolling(window).mean().shift(1)
            df_feat[f'Turnover_MA_HL_{window}'] = ratio_HL.rolling(window).mean().shift(1)
    
    # 指数关系特征
    df_feat['Price_Ratio'] = (df_feat['Close_500'] / df_feat['Close_HL']).shift(1)
    df_feat['Mom_Diff_10'] = df_feat['Mom_500_10'] - df_feat['Mom_HL_10']
    if 'Vol_500_20' in df_feat.columns and 'Vol_HL_20' in df_feat.columns:
        df_feat['Vol_Ratio'] = (df_feat['Vol_500_20'] / df_feat['Vol_HL_20']).replace([np.inf, -np.inf], np.nan)
    
    # 市场状态
    df_feat['Market_Vol'] = (df_feat['Ret_Index_500'] + df_feat['Ret_Index_HL']).rolling(20).std().shift(1)
    if 'Volume_500' in df_feat.columns and 'Volume_HL' in df_feat.columns:
        df_feat['Volume_Sum'] = (df_feat['Volume_500'] + df_feat['Volume_HL']).rolling(10).mean().shift(1)
    
    # ===== 填充缺失值 =====
    df_feat = df_feat.fillna(0)
    
    # ===== 保险2：严格筛选特征列 =====
    exclude_base = {'Close_500','Close_HL','Volume_500','Volume_HL','Turnover_500','Turnover_HL',
                   'Ret_Index_500','Ret_Index_HL','Ret_Fund_500','Ret_Fund_HL'}
    sensitive_keywords = {'code','name','symbol','index','date','time','label','prev','open','high','low','close'}
    
    feature_cols = [
        col for col in df_feat.columns 
        if col not in exclude_base 
        and not any(kw in col.lower() for kw in sensitive_keywords)
        and pd.api.types.is_numeric_dtype(df_feat[col])  # 最终验证
    ]
    
    if len(feature_cols) < 5:
        print(f"⚠️  有效特征较少 ({len(feature_cols)}个)，可用特征: {feature_cols}")
        # 至少保留基础动量特征
        fallback_cols = [col for col in df_feat.columns if 'Mom_' in col or 'Vol_' in col]
        feature_cols = list(set(feature_cols + fallback_cols))
    
    if not feature_cols:
        raise ValueError("❌ 未生成有效特征列！请检查数据结构和列名")
    
    print(f"✓ 特征工程完成 | 有效特征数: {len(feature_cols)} | 示例: {feature_cols[:3]}")
    return df_feat, feature_cols

# ======================
# 2. 标签生成（保持不变）
# ======================
def create_labels(df, horizon=1):
    label = (df['Ret_Fund_500'].shift(-horizon) - df['Ret_Fund_HL'].shift(-horizon)) / 100
    return label.dropna()

# ======================
# 3. XGBoost训练（增加数据验证）
# ======================
def train_xgboost_with_cv(X, y, n_splits=5):
    tscv = TimeSeriesSplit(n_splits=n_splits)
    models = []
    oof_preds = pd.Series(index=y.index, dtype=float)
    
    # 验证X是否纯数值
    if not np.issubdtype(X.values.dtype, np.number):
        raise TypeError("❌ 训练数据包含非数值类型！请检查特征工程")
    
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        # 标准化
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        # 训练
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            objective='reg:squarederror',
            eval_metric='rmse'
        )
        # 兼容不同 xgboost 版本的 early stopping 参数：
        fit_kwargs = {
            'eval_set': [(X_val_scaled, y_val)],
            'verbose': False
        }
        try:
            # 先尝试沿用旧版 API（支持 early_stopping_rounds）
            model.fit(X_train_scaled, y_train, early_stopping_rounds=15, **fit_kwargs)
        except TypeError:
            # 若不支持 early_stopping_rounds，则使用 callbacks（新 API）
            try:
                model.fit(X_train_scaled, y_train, callbacks=[xgb.callback.EarlyStopping(rounds=15)], **fit_kwargs)
            except Exception:
                # 最后回退：不使用 early stopping
                model.fit(X_train_scaled, y_train, **fit_kwargs)

        # 记录 OOF 预测并计算验证集 RMSE 输出（避免依赖 model.best_score 在不同版本中可能不存在）
        val_pred = model.predict(X_val_scaled)
        oof_preds.iloc[val_idx] = val_pred
        models.append((model, scaler))
        rmse = np.sqrt(np.mean((y_val - val_pred) ** 2))
        print(f"  ✓ Fold {fold+1}/{n_splits} | Val RMSE: {rmse:.6f}")
    
    final_model, final_scaler = models[-1]
    importance = pd.Series(final_model.feature_importances_, index=X.columns).sort_values(ascending=False)
    return final_model, final_scaler, importance, oof_preds

# ======================
# 4. 主回测流程（整合ML信号）
# ======================
print("="*60)
print("🚀 启动机器学习增强版流动性因子策略回测")
print("="*60)

# ----------------------------------------------------------------------
# 0. 数据加载与预处理
# ----------------------------------------------------------------------
if not os.path.exists(FILE_PATH):
    print(f"❌ 错误：找不到文件 {FILE_PATH}")
    exit()

try:
    df = pd.read_csv(FILE_PATH, parse_dates=['TradingDay'])
    print(f"✅ 成功加载数据: {len(df)} 条记录")
except Exception as e:
    print(f"❌ 无法读取文件: {e}")
    exit()

# 按照交易日排序并设置索引
df = df.set_index('TradingDay').sort_index()

# 重命名列以便后续处理
# 交易额重命名
df.rename(columns={'turnover_value1': 'Turnover_500', 'turnover_value2': 'Turnover_HL'}, inplace=True)
# 交易量重命名
df.rename(columns={'turnover_volume1': 'Volume_500', 'turnover_volume2': 'Volume_HL'}, inplace=True)
# 涨跌幅重命名
df.rename(columns={'change_pct1': 'Ret_Index_500', 'change_pct2': 'Ret_Index_HL'}, inplace=True)
# 收盘价重命名
df.rename(columns={'close_price1': 'Close_500', 'close_price2': 'Close_HL'}, inplace=True)
# 基金涨跌幅重命名
df.rename(columns={'change_pct4': 'Ret_Fund_500', 'change_pct5': 'Ret_Fund_HL'}, inplace=True)

# 只保留必要列（后续特征工程会基于这些列生成更多特征）
df = df[['Close_500', 'Close_HL', 'Volume_500', 'Volume_HL',
         'Turnover_500', 'Turnover_HL', 'Ret_Index_500', 'Ret_Index_HL', 'Ret_Fund_500', 'Ret_Fund_HL']]

# --- 步骤1: 特征工程（全量数据）---
print("\n[1/4] 构建特征工程...")
df_full, feature_cols = create_features(df)
print(f"✓ 生成特征数量: {len(feature_cols)} | 特征示例: {feature_cols[:5]}")

# --- 步骤2: 准备训练数据（回测开始前的历史数据）---
print("\n[2/4] 准备训练数据与标签...")
train_end = pd.Timestamp(BACKTEST_START_DATE) - pd.Timedelta(days=1)
df_train = df_full.loc[:train_end].copy()
y_train = create_labels(df_train, horizon=1)  # 预测下一期相对收益
X_train = df_train.loc[y_train.index, feature_cols]  # 对齐索引

print(f"  训练样本量: {len(X_train)} | 标签范围: [{y_train.min():.4f}, {y_train.max():.4f}]")

# --- 步骤3: 训练XGBoost模型 ---
print("\n[3/4] 时序交叉验证训练XGBoost模型...")
model, scaler, feature_imp, oof_preds = train_xgboost_with_cv(X_train, y_train, n_splits=5)

# 显示Top10特征
print("\n🔝 Top 10 特征重要性:")
for feat, imp in feature_imp.head(10).items():
    print(f"  • {feat:30s}: {imp:.4f}")

# --- 步骤4: 生成回测期信号 ---
print("\n[4/4] 生成回测期ML信号...")
df_bt = df_full.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()

# 用训练好的模型预测回测期（注意：预测的是"当日"相对收益，用于当日仓位）
X_bt = df_bt[feature_cols].copy()
X_bt_scaled = scaler.transform(X_bt)
ml_signal_raw = model.predict(X_bt_scaled)  # 预测值：500相对HL的预期超额收益（小数）

# 信号标准化：使用训练集OOF预测的分布进行稳健归一化（避免回测期分布偏移）
signal_mean = oof_preds.mean()
signal_std = oof_preds.std() + 1e-6
df_bt['ML_Signal_Standardized'] = (ml_signal_raw - signal_mean) / signal_std

# ======================
# 5. 仓位管理（Sigmoid调仓 - 适配ML信号）
# ======================
SIGMOID_SCALE_ML = 3.0  # ML信号波动较大，适当降低scale使调仓更平滑

def sigmoid(x, scale=SIGMOID_SCALE_ML):
    """Sigmoid函数：将标准化信号映射到[0,1]仓位比例"""
    return 1 / (1 + np.exp(-scale * x))

# 生成目标仓位（无需shift：ML信号在T-1日生成，用于T日开盘调仓）
df_bt['Target_500'] = sigmoid(df_bt['ML_Signal_Standardized'], scale=SIGMOID_SCALE_ML)
df_bt['Target_HL'] = 1 - df_bt['Target_500']

# 实际仓位 = 当日目标仓位（假设T-1日收盘后生成信号，T日开盘调仓）
df_bt['Position_500'] = df_bt['Target_500'].fillna(0.5)  # 首日无信号时用0.5
df_bt['Position_HL'] = df_bt['Target_HL'].fillna(0.5)

# ======================
# 6. 回测执行及绩效计算（与原逻辑一致）
# ======================
# 日收益率（注意：原始Ret_Fund为百分比，需/100）
df_bt['Strategy_Return'] = (df_bt['Position_500'] * df_bt['Ret_Fund_500'] + 
                            df_bt['Position_HL'] * df_bt['Ret_Fund_HL']) / 100
df_bt['Benchmark_Return'] = (0.5 * df_bt['Ret_Fund_500'] + 
                             0.5 * df_bt['Ret_Fund_HL']) / 100

# 累计净值
df_bt['Cumulative_Strategy_Return'] = (1 + df_bt['Strategy_Return']).cumprod()
df_bt['Cumulative_Benchmark_Return'] = (1 + df_bt['Benchmark_Return']).cumprod()
df_bt['Excess_Return'] = df_bt['Cumulative_Strategy_Return'] - df_bt['Cumulative_Benchmark_Return']

# 最大回撤
df_bt['Roll_Max'] = df_bt['Cumulative_Strategy_Return'].cummax()
df_bt['Drawdown'] = df_bt['Cumulative_Strategy_Return'] / df_bt['Roll_Max'] - 1
max_dd = df_bt['Drawdown'].min()

# ======================
# 7. 绩效输出与可视化
# ======================
# 总绩效
strategy_ret = df_bt['Cumulative_Strategy_Return'].iloc[-1] - 1
benchmark_ret = df_bt['Cumulative_Benchmark_Return'].iloc[-1] - 1
excess_ret = strategy_ret - benchmark_ret

print("\n" + "="*50)
print("📊 机器学习策略回测绩效摘要")
print("="*50)
print(f"📈 策略总收益率:        {strategy_ret*100:8.2f}%")
print(f"📈 基准总收益率:        {benchmark_ret*100:8.2f}%")
print(f"🔥 相对超额收益:        {excess_ret*100:8.2f}%")
print(f"📉 最大回撤:            {max_dd*100:8.2f}%")
print(f"🤖 信号来源:            XGBoost (特征数: {len(feature_cols)})")
print("="*50)

# 区间统计
print(f"\n📅 特定区间统计: 【 {STRATEGY_START_DATE} 至今 】")
print("-"*50)
try:
    df_spec = df_bt.loc[STRATEGY_START_DATE:]
    if not df_spec.empty and len(df_spec) > 1:
        s_ret = df_spec['Cumulative_Strategy_Return'].iloc[-1] / df_spec['Cumulative_Strategy_Return'].iloc[0] - 1
        b_ret = df_spec['Cumulative_Benchmark_Return'].iloc[-1] / df_spec['Cumulative_Benchmark_Return'].iloc[0] - 1
        excess_spec = s_ret - b_ret
        print(f"   🔹 策略区间收益: {s_ret:.2%}")
        print(f"   🔹 基准区间收益: {b_ret:.2%}")
        print(f"   🔥 区间超额收益: {excess_spec:.2%}")
    else:
        print("   ⚠️ 数据不足，无法计算区间收益")
except Exception as e:
    print(f"   ⚠️ 区间统计错误: {e}")

# ======================
# 8. 可视化
# ======================
plt.figure(figsize=(14, 10))

# 子图1: 累计收益对比
plt.subplot(3, 1, 1)
plt.plot(df_bt.index, df_bt['Cumulative_Strategy_Return'], label='XGBoost策略', linewidth=2, color='#1f77b4')
plt.plot(df_bt.index, df_bt['Cumulative_Benchmark_Return'], label='50-50基准', linewidth=2, color='#ff7f0e')
plt.title('📈 累计净值曲线 (XGBoost增强策略 vs 基准)', fontsize=14, fontweight='bold')
plt.ylabel('累计净值')
plt.legend()
plt.grid(alpha=0.3)

# 子图2: 超额收益与信号
plt.subplot(3, 1, 2)
plt.plot(df_bt.index, df_bt['Excess_Return'], label='累计超额收益', color='green', linewidth=2)
plt.twinx()
plt.plot(df_bt.index, df_bt['ML_Signal_Standardized'], label='ML标准化信号', color='purple', alpha=0.7, linestyle='--')
plt.title('💡 累计超额收益与ML信号', fontsize=14)
plt.ylabel('超额收益 / 信号值')
plt.legend(loc='upper left')
plt.grid(alpha=0.3)

# 子图3: 仓位分布
plt.subplot(3, 1, 3)
plt.plot(df_bt.index, df_bt['Position_500'], label='中证500仓位', linewidth=2)
plt.plot(df_bt.index, df_bt['Position_HL'], label='红利低波仓位', linewidth=2)
plt.axhline(0.5, color='k', linestyle='--', alpha=0.3, label='均衡线')
plt.title('⚖️ 动态仓位分布', fontsize=14)
plt.ylabel('仓位比例')
plt.xlabel('日期')
plt.legend()
plt.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('results/liq/backtest_results_xgboost_enhanced.png', dpi=300, bbox_inches='tight')
print("\n✅ 回测图表已保存: results/liq/backtest_results_xgboost_enhanced.png")
plt.show()

# ======================
# 9. 附加：特征重要性可视化（可选）
# ======================
plt.figure(figsize=(10, 6))
feature_imp.head(15).plot(kind='barh', color='steelblue')
plt.gca().invert_yaxis()
plt.title('🔝 XGBoost Top 15 特征重要性', fontsize=14, fontweight='bold')
plt.xlabel('重要性得分')
plt.tight_layout()
plt.savefig('results/liq/feature_importance.png', dpi=300, bbox_inches='tight')
print("✅ 特征重要性图已保存: results/liq/feature_importance.png")
plt.show()

print("\n" + "="*60)
print("✨ 回测完成！策略已成功升级为机器学习增强版")
print("💡 建议下一步：")
print("   1. 检查特征重要性，优化特征工程")
print("   2. 调整XGBoost超参数（max_depth, learning_rate）")
print("   3. 尝试滚动窗口重新训练（每季度更新模型）")
print("   4. 添加交易成本模拟（滑点、手续费）")
print("="*60)