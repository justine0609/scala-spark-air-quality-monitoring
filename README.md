# 区域空气质量监测与污染特征分析

> Regional Air Quality Monitoring & Pollution Characteristic Analysis

基于 **Apache Spark (Scala)** 的空气质量大数据分析项目：从多源异构数据接入、脏数据清洗、AQI 口径标准化，到九项统计指标计算与可视化大屏，覆盖了大数据分析的完整流程。

![Scala](https://img.shields.io/badge/Scala-2.12.18-DC322F?logo=scala&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.5.9-E25A1C?logo=apachespark&logoColor=white)
![JDK](https://img.shields.io/badge/JDK-11%2F17-007396)
![sbt](https://img.shields.io/badge/sbt-1.9.x-4C8BF5)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 项目简介

本项目以某区域 30 个监测站点、60 天（2024-10-01 ~ 2024-11-29）共 **9000 条逐时空气质量记录**为数据源，结合站点台账与气象数据，完成：

1. **数据接入与探查** —— 四张异构 CSV 表的读取、字段盘点、值域与非规范取值分析；
2. **数据清洗** —— 浓度单位还原、负值/异常置空、AQI 重算与偏差校验、站点编号归一、去重与关联，每一步都有明确的剔除/置空计数；
3. **口径标准化** —— 基于《环境空气质量指数（AQI）技术规定》重算 AQI，用区间关联（非 if-else）判定等级、以 IAQI 定首要污染物；
4. **指标计算** —— 九项统计指标（核心 KPI、优良率、等级分布、首要污染物、区县对比、站点排名、月度对比、气象效应等）；
5. **可视化大屏** —— 1920×1080 自适应大屏，8 个面板 + 4 张指标卡，数据全部来自运行时生成的 JSON，顶部结论由数据实时推导。

**核心结论**：统计期优良率 **85.00%**（优良 51 天 / 有效 60 天），重污染 4 天，AQI 峰值 246（2024-11-11）；首要污染物为 **PM2.5**（期间均值 66.6 μg/m³）；11 月较 10 月 PM2.5 均值上升 32.34%；风速与 AQI 弱负相关（r = -0.0811）。

---

## 技术栈

| 层 | 技术 |
|------|------|
| 语言 | Scala 2.12.18 |
| 计算引擎 | Apache Spark 3.5.9（DataFrame / Dataset API） |
| 构建工具 | sbt 1.9.x |
| 运行时 | JDK 11 / 17（Spark 3.5 支持 8/11/17） |
| 数据存储 | CSV（原始）→ Parquet（清洗后明细）→ JSON（指标结果） |
| 可视化 | ECharts（本地副本，离线可用）+ 原生 JS + JDK 内置 HttpServer |

---

## 目录结构

```
.
├── build.sbt                     构建配置（含 Hadoop 路径注入、JDK 版本自适应）
├── run.bat                       一键运行（Windows，自动处理中文路径等环境问题）
├── 打开大屏.bat                  一键起静态服务并打开可视化大屏
├── 运行说明.md                   详细中文运行文档（环境要求/步骤/FAQ）
├── data/                         四份原始 CSV（只读，字节未改动）
│   ├── hourly_measurements.csv   逐时空气质量（9000 条）
│   ├── stations.csv              站点台账
│   ├── meteorology.csv           气象数据
│   └── aqi_levels.csv            AQI 等级区间表
├── output/                       11 个指标 JSON（一图一文件，统一 {meta,data} 结构）
├── dashboard/                    可视化大屏
│   ├── index.html                大屏页面
│   ├── app.js                    前端逻辑（数据加载/渲染/轮播/刷新）
│   ├── style.css                 样式
│   ├── Server.java               零依赖静态服务（JDK HttpServer，替代 Python）
│   └── lib/echarts.min.js        ECharts 本地副本
└── src/main/scala/aqi/           源码（分层清晰，共 11 个文件）
    ├── Main.scala                入口，调度 check / probe / clean / metrics
    ├── Config.scala              配置与环境自愈（Hadoop 原生库兜底）
    ├── Model.scala               领域模型 + IAQI/AQI 计算 + 等级区间关联
    ├── TimeUtil.scala            时间解析（兼容四种时间写法）
    ├── Reader.scala              数据读取层
    ├── Probe.scala               探查层（四表接入与质量统计）
    ├── Cleaner.scala             清洗层（八步流水线）
    ├── Metrics.scala             指标层（九项指标）
    ├── Json.scala / Writer.scala 输出层（序列化与落盘）
    └── Report.scala              展示层（表格渲染）
```

---

## 快速开始

### 前置依赖

唯一需要手工安装的是 **JDK 11 或 17** 与 **sbt 1.9.x**；Scala、Spark 均由 sbt 自动下载。

### 运行

**Windows（推荐）**：双击 `run.bat`，自动完成环境自检并跑完整流程。

**命令行**：

```bash
cd 1_项目源代码
sbt "run check"      # 1. 环境自检（可选，先跑一次看环境是否就绪）
sbt "run probe"      # 2. 数据探查（只读统计）
sbt "run clean"      # 3. 单独查看清洗过程
sbt "run metrics"    # 4. 完整流水线（产出 cleaned/ 与 output/）
```

首次运行需从 Maven 仓库下载 Spark 依赖（约 300 MB），之后每次约 30～45 秒。

### 打开可视化大屏

> ⚠️ 不要直接双击 `dashboard/index.html`（`file://` 协议下 `fetch` 读 JSON 会被浏览器同源策略拦截，表现为白屏）。

Windows 双击 `打开大屏.bat`，或手动起一个静态服务：

```bash
java dashboard/Server.java 8080        # 需要 JDK 11+
# 或
python -m http.server 8080
```

浏览器访问 `http://127.0.0.1:8080/`，页面按 1920×1080 排版并随窗口等比缩放。

---

## 核心设计要点

- **AQI 重算而非照搬**：平台 AQI 有 188 条无效、128 条偏差超 10，全部按国标分段线性公式重算（316 条），其余 8684 条保留平台值，并在清洗计数中明确说明。
- **等级判定用区间关联**：`aqi_levels.csv` 作为区间表参与 join（闭区间 `lo ≤ aqi ≤ hi`），而非散落各处的 if-else；AQI 先取整再归级，避免出现落不进任何档的浮点值。
- **首要污染物按 IAQI**：取六项污染物 IAQI 最大值对应的污染物，符合国标定义。
- **优良率口径严谨**：分母为"有效天数"而非"总天数"，日均值要求当日有效记录 ≥ 3 条才参与统计。
- **未全量 collect 单机处理**：指标聚合全部用 DataFrame/窗口函数在 Spark 上完成，只有最终结果才收集回 Driver 落盘。
- **换机可运行**：`run.bat` 自动把 `java.io.tmpdir` 与 `sbt.global.base` 指向纯英文路径，规避中文 Windows 用户名导致的 sbt 编译错误（详见《运行说明.md》）。

---

## 关于 Windows 上的 Hadoop 原生库

Spark 在 Windows 写 Parquet 依赖 `winutils.exe` 与 `hadoop.dll`。本仓库未附带这两个二进制文件，若在 Windows 上运行，请自行下载并将它们放入 `hadoop/bin/` 目录（工程已通过 `build.sbt` 的 `-Dhadoop.home.dir` 与 `-Djava.library.path` 指向该目录）：

- 下载地址：[cdarlint/winutils](https://github.com/cdarlint/winutils)（选择与 Hadoop 版本匹配的 `winutils.exe` 与 `hadoop.dll`）

> 在 Linux / macOS 上运行通常无需这两个文件。

---

## 输出说明

| 输出 | 内容 |
|------|------|
| `cleaned/hourly_detail` | 9000 行逐时明细（含重算 AQI、站点台账字段） |
| `cleaned/station_daily` | 1800 行站点日均 |
| `cleaned/county_daily` | 360 行区县日均（含等级与气象） |
| `cleaned/city_daily` | 60 行城市日均 |
| `output/*.json` | 11 个指标文件，一图一文件，统一 `{meta, data}` 结构 |

`cleaned/` 由 `sbt "run metrics"` 生成（未随仓库提交）；`output/` 已包含一份完整运行结果，可直接查看或打开大屏。

---

## 数据来源

`data/` 下四份 CSV 为课程作业提供的原始数据，未经修改。本项目仅用于学习与演示。

## 许可证

[MIT](LICENSE)
