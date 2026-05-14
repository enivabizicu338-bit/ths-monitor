# THS-Monitor - 同花顺热榜监控系统

实时监控同花顺热榜数据，智能推荐股票，追踪排名变化。

## 功能特性

- **实时数据采集**: 直接对接同花顺热榜API
- **SQLite存储**: 本地持久化保存历史数据
- **智能推荐**: 基于排名变化、热度、涨幅等多因子评分
- **排名追踪**: 记录股票首次上榜时间、在榜时长、排名趋势
- **回测框架**: 分析推荐准确率，优化推荐策略
- **自动采集**: 交易时段每5分钟自动采集

## 快速开始

\\\ash
# 安装依赖
pip install fastapi uvicorn aiohttp requests

# 启动服务
python ths_monitor.py

# 访问 http://localhost:8000
\\\

## 数据源

- 同花顺热榜: https://eq.10jqka.com.cn/frontend/thsTopRank/index.html
- 人气榜: https://basic.10jqka.com.cn/basicph/popularityRanking.html

## 技术栈

- 后端: FastAPI + SQLite
- 前端: 原生 HTML/CSS/JS
- 数据: 同花顺公开API
