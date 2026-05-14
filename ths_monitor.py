"""
THS-Monitor v1.0 - 同花顺热榜监控与A股短线交易辅助系统
========================================================
核心功能：
1. 实时采集同花顺热榜数据（小时榜/日榜/飙升榜/概念板块）
2. SQLite持久化存储，追踪排名变化历史
3. 分析上榜规律：首次上榜时间、在榜时长、板块上榜数、上榜后表现
4. 基于排名变化推荐股票
5. 回测推荐准确性，学习优化推荐因子

数据源：同花顺公开API（无需认证）
- 热榜：https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock
- 人气榜：https://basic.10jqka.com.cn/api/stockph/popularity/top/
- 概念板块：https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/plate
"""
import asyncio, json, re, logging, os, time, sqlite3, math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import aiohttp, uvicorn, requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("THS-Monitor")

app = FastAPI(title="THS-Monitor", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# ============ 配置 ============
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ths_monitor.db")
SNAPSHOT_INTERVAL = 300  # 快照间隔（秒），交易时段每5分钟采集一次
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://eq.10jqka.com.cn/frontend/thsTopRank/index.html',
}

# 同花顺API端点
THS_HOT_LIST = "https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock"
THS_PLATE_LIST = "https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/plate"
THS_POPULARITY = "https://basic.10jqka.com.cn/api/stockph/popularity/top/"

# ============ SQLite 数据库 ============
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """初始化数据库表结构"""
    with get_db() as conn:
        conn.executescript("""
            -- 排名快照表（每次采集保存一次）
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                list_type TEXT NOT NULL,
                data_type TEXT NOT NULL DEFAULT 'hour',
                raw_json TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(ts);
            CREATE INDEX IF NOT EXISTS idx_snapshots_type ON snapshots(list_type, data_type);

            -- 股票排名明细表（从快照中解析）
            CREATE TABLE IF NOT EXISTS stock_rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER,
                ts TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                market TEXT,
                rank_order INTEGER,
                heat_score REAL,
                change_pct REAL,
                rank_change INTEGER,
                popularity_tag TEXT,
                concept_tags TEXT,
                list_type TEXT NOT NULL,
                data_type TEXT DEFAULT 'hour',
                UNIQUE(snapshot_id, code, list_type)
            );
            CREATE INDEX IF NOT EXISTS idx_rankings_code ON stock_rankings(code);
            CREATE INDEX IF NOT EXISTS idx_rankings_ts ON stock_rankings(ts);
            CREATE INDEX IF NOT EXISTS idx_rankings_type ON stock_rankings(list_type);

            -- 股票追踪表（首次发现时创建）
            CREATE TABLE IF NOT EXISTS stock_tracker (
                code TEXT PRIMARY KEY,
                name TEXT,
                first_seen_ts TEXT,
                last_seen_ts TEXT,
                total_snapshots INTEGER DEFAULT 0,
                total_hours_on_board REAL DEFAULT 0,
                best_rank INTEGER DEFAULT 999,
                avg_rank REAL DEFAULT 0,
                max_heat_score REAL DEFAULT 0,
                concept_tags TEXT,
                status TEXT DEFAULT 'active',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_tracker_status ON stock_tracker(status);

            -- 板块追踪表
            CREATE TABLE IF NOT EXISTS plate_tracker (
                code TEXT PRIMARY KEY,
                name TEXT,
                plate_type TEXT,
                first_seen_ts TEXT,
                last_seen_ts TEXT,
                total_snapshots INTEGER DEFAULT 0,
                best_rank INTEGER DEFAULT 999,
                hot_tag TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- 推荐记录表
            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                name TEXT,
                reason TEXT,
                score REAL,
                rec_type TEXT,
                created_at TEXT NOT NULL,
                -- 回测字段（推荐后填充）
                next_day_change_pct REAL,
                next_3day_change_pct REAL,
                next_5day_change_pct REAL,
                max_change_pct_5d REAL,
                actual_outcome TEXT DEFAULT 'pending',
                updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_recs_code ON recommendations(code);
            CREATE INDEX IF NOT EXISTS idx_recs_type ON recommendations(rec_type);
            CREATE INDEX IF NOT EXISTS idx_recs_outcome ON recommendations(actual_outcome);

            -- 每日收盘价表（用于回测）
            CREATE TABLE IF NOT EXISTS daily_prices (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                close REAL,
                change_pct REAL,
                PRIMARY KEY(code, date)
            );
            CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(date);
        """)
    logger.info("数据库初始化完成")


# ============ 数据采集 ============
async def fetch_ths_hot_list(session, list_type="normal", data_type="hour") -> Dict:
    """获取同花顺热榜数据"""
    params = {'stock_type': 'a', 'type': data_type, 'list_type': list_type}
    try:
        async with session.get(THS_HOT_LIST, params=params, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("status_code") == 0 and data.get("data", {}).get("stock_list"):
                    return {"success": True, "list_type": list_type, "data_type": data_type,
                            "stocks": data["data"]["stock_list"], "count": len(data["data"]["stock_list"])}
    except Exception as e:
        logger.error(f"热榜获取失败: {e}")
    return {"success": False, "list_type": list_type, "data_type": data_type, "stocks": [], "count": 0}


async def fetch_ths_plate(session, plate_type="concept") -> Dict:
    """获取概念/行业板块热榜"""
    try:
        async with session.get(THS_PLATE_LIST, params={'type': plate_type}, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("status_code") == 0 and data.get("data", {}).get("plate_list"):
                    return {"success": True, "plate_type": plate_type,
                            "plates": data["data"]["plate_list"], "count": len(data["data"]["plate_list"])}
    except Exception as e:
        logger.error(f"板块热榜获取失败: {e}")
    return {"success": False, "plate_type": plate_type, "plates": [], "count": 0}


def fetch_popularity_sync() -> Dict:
    """获取人气榜（同步，因为basic.10jqka可能限制异步）"""
    try:
        r = requests.get(THS_POPULARITY, headers={**HEADERS,
            'Referer': 'https://basic.10jqka.com.cn/basicph/popularityRanking.html'}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get("status_code") == 0 and data.get("data", {}).get("list"):
                return {"success": True, "stocks": data["data"]["list"], "count": len(data["data"]["list"])}
    except Exception as e:
        logger.error(f"人气榜获取失败: {e}")
    return {"success": False, "stocks": [], "count": 0}


def save_snapshot(list_type: str, data_type: str, stocks: list):
    """保存快照到数据库"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_json = json.dumps(stocks, ensure_ascii=False)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        # 保存原始快照
        cur = conn.execute("INSERT INTO snapshots (ts, list_type, data_type, raw_json) VALUES (?,?,?,?)",
                          (ts, list_type, data_type, raw_json))
        snap_id = cur.lastrowid
        
        # 解析并保存排名明细
        for i, s in enumerate(stocks):
            try:
                code = str(s.get("code", ""))
                name = str(s.get("name", ""))
                market = str(s.get("market", ""))
                rank_order = s.get("order", 0)
                heat_score = float(s.get("rate", 0) or 0)
                change_pct = float(s.get("rise_and_fall", 0) or 0)
                rank_change = s.get("hot_rank_chg", 0)
                tag = s.get("tag", {}) or {}
                pop_tag = str(tag.get("popularity_tag", ""))
                concept_tags = json.dumps(tag.get("concept_tag", []), ensure_ascii=False)
                
                conn.execute("""INSERT OR REPLACE INTO stock_rankings 
                    (snapshot_id, ts, code, name, market, rank_order, heat_score, change_pct, 
                     rank_change, popularity_tag, concept_tags, list_type, data_type)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (snap_id, ts, code, name, market, rank_order, heat_score, change_pct,
                     rank_change, pop_tag, concept_tags, list_type, data_type))
                
                # 更新追踪表
                tracker = conn.execute("SELECT * FROM stock_tracker WHERE code=?", (code,)).fetchone()
                if tracker:
                    total = tracker["total_snapshots"] + 1
                    avg_rank = (tracker["avg_rank"] * tracker["total_snapshots"] + rank_order) / total
                    conn.execute("""UPDATE stock_tracker SET 
                        name=?, last_seen_ts=?, total_snapshots=?,
                        best_rank=MIN(best_rank, ?), avg_rank=?,
                        max_heat_score=MAX(max_heat_score, ?),
                        concept_tags=?, updated_at=CURRENT_TIMESTAMP
                        WHERE code=?""",
                        (name, ts, total, rank_order, round(avg_rank, 1),
                         heat_score, concept_tags, code))
                else:
                    conn.execute("""INSERT OR IGNORE INTO stock_tracker 
                        (code, name, first_seen_ts, last_seen_ts, total_snapshots,
                         best_rank, avg_rank, max_heat_score, concept_tags)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                        (code, name, ts, ts, 1, rank_order, rank_order, heat_score, concept_tags))
            except Exception as e:
                logger.error(f"保存股票 {s.get('code','?')} 失败: {e}")
                continue
        
        conn.commit()
        logger.info(f"快照保存成功: {list_type}/{data_type} {len(stocks)}只股票 snap_id={snap_id}")
    except Exception as e:
        logger.error(f"快照保存失败: {list_type}/{data_type}: {e}")
        conn.rollback()
    finally:
        conn.close()


def save_plate_snapshot(plate_type: str, plates: list):
    """保存板块快照"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        for p in plates:
            code = str(p.get("code", ""))
            name = str(p.get("name", ""))
            hot_tag = str(p.get("hot_tag", ""))
            rank_order = p.get("order", 0)
            
            tracker = conn.execute("SELECT * FROM plate_tracker WHERE code=?", (code,)).fetchone()
            if tracker:
                conn.execute("""UPDATE plate_tracker SET 
                    name=?, last_seen_ts=?, total_snapshots=total_snapshots+1,
                    best_rank=MIN(best_rank, ?), hot_tag=?, updated_at=CURRENT_TIMESTAMP
                    WHERE code=?""",
                    (name, ts, rank_order, hot_tag, code))
            else:
                conn.execute("""INSERT OR REPLACE INTO plate_tracker 
                    (code, name, plate_type, first_seen_ts, last_seen_ts, total_snapshots, best_rank, hot_tag)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (code, name, plate_type, ts, ts, 1, rank_order, hot_tag))
        conn.commit()


# ============ 分析引擎 ============
def get_stock_analysis(code: str) -> Dict:
    """获取单只股票的完整分析"""
    with get_db() as conn:
        tracker = conn.execute("SELECT * FROM stock_tracker WHERE code=?", (code,)).fetchone()
        if not tracker:
            return {"code": code, "found": False}
        
        # 排名历史
        rankings = conn.execute("""
            SELECT ts, rank_order, heat_score, change_pct, rank_change, popularity_tag, list_type
            FROM stock_rankings WHERE code=? ORDER BY ts
        """, (code,)).fetchall()
        
        # 计算在榜时长
        first_ts = tracker["first_seen_ts"]
        last_ts = tracker["last_seen_ts"]
        try:
            first_dt = datetime.strptime(first_ts, "%Y-%m-%d %H:%M:%S")
            last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
            hours_on_board = (last_dt - first_dt).total_seconds() / 3600
            days_on_board = hours_on_board / 24
        except:
            hours_on_board = 0
            days_on_board = 0
        
        # 排名趋势
        rank_trend = [{"ts": r["ts"], "rank": r["rank_order"], "heat": r["heat_score"],
                       "pct": r["change_pct"], "chg": r["rank_change"]} for r in rankings[-50:]]
        
        # 板块信息
        concepts = json.loads(tracker["concept_tags"]) if tracker["concept_tags"] else []
        
        # 同板块上榜数
        same_plate_stocks = {}
        for c in concepts:
            same = conn.execute("""
                SELECT DISTINCT code FROM stock_rankings 
                WHERE concept_tags LIKE ? AND ts >= ?
            """, (f'%{c}%', first_ts)).fetchall()
            same_plate_stocks[c] = len(same)
        
        return {
            "code": code,
            "name": tracker["name"],
            "found": True,
            "first_seen": first_ts,
            "last_seen": last_ts,
            "hours_on_board": round(hours_on_board, 1),
            "days_on_board": round(days_on_board, 1),
            "total_snapshots": tracker["total_snapshots"],
            "best_rank": tracker["best_rank"],
            "avg_rank": round(tracker["avg_rank"], 1),
            "max_heat": tracker["max_heat_score"],
            "concepts": concepts,
            "same_plate_count": same_plate_stocks,
            "rank_trend": rank_trend,
        }


def get_recommendations() -> List[Dict]:
    """基于排名变化生成推荐"""
    with get_db() as conn:
        # 获取最近两次快照的排名变化
        recent = conn.execute("""
            SELECT code, name, rank_order, heat_score, change_pct, rank_change,
                   popularity_tag, concept_tags, ts
            FROM stock_rankings 
            WHERE list_type='normal' AND data_type='hour'
            ORDER BY ts DESC, rank_order ASC
            LIMIT 200
        """).fetchall()
        
        if len(recent) < 2:
            return []
        
        recs = []
        seen = set()
        for r in recent:
            code = r["code"]
            if code in seen:
                continue
            seen.add(code)
            
            score = 0
            reasons = []
            
            # 1. 新上榜或排名大幅上升
            chg = r["rank_change"] or 0
            if chg > 20:
                score += 30
                reasons.append(f"飙升{chg}位")
            elif chg > 10:
                score += 20
                reasons.append(f"上升{chg}位")
            elif chg > 0:
                score += 10
                reasons.append(f"上升{chg}位")
            
            # 2. 持续上榜
            tag = r["popularity_tag"] or ""
            if "持续" in tag:
                score += 25
                reasons.append("持续上榜")
            if "首板" in tag or "涨停" in tag:
                score += 20
                reasons.append("首板涨停")
            
            # 3. 高热度
            heat = r["heat_score"] or 0
            if heat > 100000:
                score += 15
                reasons.append("超高热度")
            elif heat > 50000:
                score += 10
                reasons.append("高热度")
            
            # 4. 涨幅适中（非追高）
            pct = r["change_pct"] or 0
            if 0 < pct < 5:
                score += 10
                reasons.append("温和上涨")
            elif pct >= 9.8:
                score += 5
                reasons.append("涨停")
            
            # 5. 概念板块热度
            concepts = json.loads(r["concept_tags"]) if r["concept_tags"] else []
            if concepts:
                score += min(10, len(concepts) * 3)
                reasons.append(f"{len(concepts)}个概念")
            
            if score >= 30 and reasons:
                recs.append({
                    "code": code, "name": r["name"],
                    "score": score, "reasons": reasons,
                    "heat": heat, "change_pct": pct,
                    "rank": r["rank_order"], "rank_change": chg,
                    "tag": tag, "concepts": concepts,
                })
        
        recs.sort(key=lambda x: x["score"], reverse=True)
        return recs[:30]


def get_backtest_results() -> Dict:
    """回测推荐结果"""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM recommendations WHERE actual_outcome='pending'").fetchone()[0]
        success = conn.execute("SELECT COUNT(*) FROM recommendations WHERE actual_outcome='success'").fetchone()[0]
        fail = conn.execute("SELECT COUNT(*) FROM recommendations WHERE actual_outcome='fail'").fetchone()[0]
        
        # 按推荐类型统计
        by_type = {}
        for row in conn.execute("SELECT rec_type, COUNT(*), AVG(next_day_change_pct), AVG(next_3day_change_pct), SUM(CASE WHEN actual_outcome='success' THEN 1 ELSE 0 END) FROM recommendations WHERE actual_outcome != 'pending' GROUP BY rec_type").fetchall():
            by_type[row[0]] = {
                "count": row[1],
                "avg_next_day": round(row[2], 2) if row[2] else 0,
                "avg_3day": round(row[3], 2) if row[3] else 0,
                "success_rate": round(row[4] / row[1] * 100, 1) if row[1] > 0 else 0,
            }
        
        # 按推荐原因统计
        by_reason = {}
        for row in conn.execute("""
            SELECT reason, COUNT(*), AVG(next_day_change_pct), SUM(CASE WHEN actual_outcome='success' THEN 1 ELSE 0 END)
            FROM recommendations WHERE actual_outcome != 'pending'
            GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 20
        """).fetchall():
            by_reason[row[0]] = {
                "count": row[1],
                "avg_next_day": round(row[2], 2) if row[2] else 0,
                "success_rate": round(row[3] / row[1] * 100, 1) if row[1] > 0 else 0,
            }
        
        return {
            "total": total, "pending": pending, "success": success, "fail": fail,
            "success_rate": round(success / max(success + fail, 1) * 100, 1),
            "by_type": by_type, "by_reason": by_reason,
        }


# ============ API 端点 ============
@app.get("/")
async def root():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index_monitor.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, media_type="text/html")
    return {"name": "THS-Monitor", "version": "1.0.0", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "1.0.0", "db": os.path.exists(DB_PATH)}


@app.get("/api/status")
async def status():
    with get_db() as conn:
        total_snaps = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        tracked_stocks = conn.execute("SELECT COUNT(*) FROM stock_tracker").fetchone()[0]
        tracked_plates = conn.execute("SELECT COUNT(*) FROM plate_tracker").fetchone()[0]
        total_recs = conn.execute("SELECT COUNT(*) FROM recommendations").fetchone()[0]
        last_snap = conn.execute("SELECT ts FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
    return {
        "version": "1.0.0", "db_exists": os.path.exists(DB_PATH),
        "total_snapshots": total_snaps, "tracked_stocks": tracked_stocks,
        "tracked_plates": tracked_plates, "total_recommendations": total_recs,
        "last_snapshot": last_snap["ts"] if last_snap else None,
        "snapshot_interval": SNAPSHOT_INTERVAL,
    }


# ---------- 实时数据 ----------
@app.get("/api/hot/{list_type}")
async def hot_list(list_type: str = "normal"):
    """热榜实时数据"""
    data_type = "hour"
    async with aiohttp.ClientSession() as session:
        result = await fetch_ths_hot_list(session, list_type, data_type)
    return result


@app.get("/api/hot_day")
async def hot_day():
    """日榜数据"""
    async with aiohttp.ClientSession() as session:
        result = await fetch_ths_hot_list(session, "normal", "day")
    return result


@app.get("/api/popularity")
async def popularity():
    """人气榜"""
    return fetch_popularity_sync()


@app.get("/api/plates/{plate_type}")
async def plates(plate_type: str = "concept"):
    """板块热榜"""
    async with aiohttp.ClientSession() as session:
        result = await fetch_ths_plate(session, plate_type)
    return result


# ---------- 采集控制 ----------
@app.post("/api/collect")
async def collect():
    """手动触发一次采集（同步执行，确保数据保存）"""
    try:
        await run_collection_cycle()
        return {"status": "completed", "message": "采集完成"}
    except Exception as e:
        logger.error(f"采集失败: {e}")
        return {"status": "error", "message": str(e)}


async def run_collection_cycle():
    """执行一次完整采集"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"开始采集: {ts}")
    saved_count = 0

    async with aiohttp.ClientSession() as session:
        # 并发采集多个榜单
        tasks = [
            fetch_ths_hot_list(session, "normal", "hour"),
            fetch_ths_hot_list(session, "skyrocket", "hour"),
            fetch_ths_hot_list(session, "normal", "day"),
            fetch_ths_plate(session, "concept"),
            fetch_ths_plate(session, "industry"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logger.error(f"采集异常: {r}")
                continue
            try:
                if r.get("success") and r.get("stocks"):
                    save_snapshot(r["list_type"], r["data_type"], r["stocks"])
                    saved_count += len(r["stocks"])
                elif r.get("success") and r.get("plates"):
                    save_plate_snapshot(r["plate_type"], r["plates"])
                    saved_count += len(r["plates"])
            except Exception as e:
                logger.error(f"保存异常: {e}")

    # 人气榜（同步）
    try:
        pop = fetch_popularity_sync()
        if pop.get("success") and pop.get("stocks"):
            save_snapshot("popularity", "hour", pop["stocks"])
            saved_count += len(pop["stocks"])
    except Exception as e:
        logger.error(f"人气榜保存异常: {e}")

    logger.info(f"采集完成，共保存 {saved_count} 条记录")


# ---------- 分析 ----------
@app.get("/api/analysis/{code}")
async def analysis(code: str):
    """单股分析"""
    return get_stock_analysis(code)


@app.get("/api/tracked")
async def tracked():
    """已追踪股票列表"""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM stock_tracker ORDER BY total_snapshots DESC LIMIT 100
        """).fetchall()
        return {"total": len(rows), "stocks": [dict(r) for r in rows]}


@app.get("/api/recommendations")
async def recommendations():
    """当前推荐"""
    recs = get_recommendations()
    return {"success": True, "recommendations": recs, "count": len(recs)}


@app.post("/api/recommendations/save")
async def save_recommendations():
    """保存当前推荐到数据库（用于回测）"""
    recs = get_recommendations()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        for r in recs:
            conn.execute("""INSERT INTO recommendations 
                (code, name, reason, score, rec_type, created_at)
                VALUES (?,?,?,?,?,?)""",
                (r["code"], r["name"], ",".join(r["reasons"]), r["score"],
                 ",".join(r["concepts"]), ts))
        conn.commit()
    return {"status": "saved", "count": len(recs)}


@app.get("/api/backtest")
async def backtest():
    """回测结果"""
    return get_backtest_results()


@app.get("/api/history/{code}")
async def history(code: str, days: int = Query(7, ge=1, le=90)):
    """股票排名历史"""
    with get_db() as conn:
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT ts, rank_order, heat_score, change_pct, rank_change, popularity_tag, list_type
            FROM stock_rankings WHERE code=? AND ts>=? ORDER BY ts
        """, (code, since)).fetchall()
        return {"code": code, "days": days, "records": [dict(r) for r in rows]}


@app.get("/api/plates/tracked")
async def tracked_plates():
    """已追踪板块"""
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM plate_tracker ORDER BY total_snapshots DESC").fetchall()
        return {"total": len(rows), "plates": [dict(r) for r in rows]}


# ============ 自动采集 ============
_auto_collect_task = None

async def auto_collect_loop():
    """交易时段自动采集循环"""
    global _auto_collect_task
    while True:
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        weekday = now.weekday()
        # 交易时段: 周一到周五, 9:15-15:00
        is_trading = weekday < 5 and ((hour == 9 and minute >= 15) or (10 <= hour <= 14) or (hour == 15 and minute == 0))
        if is_trading:
            try:
                await run_collection_cycle()
            except Exception as e:
                logger.error(f"自动采集失败: {e}")
        await asyncio.sleep(SNAPSHOT_INTERVAL)


@app.post("/api/auto_collect/start")
async def start_auto_collect():
    """启动自动采集"""
    global _auto_collect_task
    if _auto_collect_task is None or _auto_collect_task.done():
        _auto_collect_task = asyncio.create_task(auto_collect_loop())
        return {"status": "started", "interval": SNAPSHOT_INTERVAL}
    return {"status": "already_running"}


@app.post("/api/auto_collect/stop")
async def stop_auto_collect():
    """停止自动采集"""
    global _auto_collect_task
    if _auto_collect_task and not _auto_collect_task.done():
        _auto_collect_task.cancel()
        _auto_collect_task = None
        return {"status": "stopped"}
    return {"status": "not_running"}


@app.get("/api/auto_collect/status")
async def auto_collect_status():
    """自动采集状态"""
    global _auto_collect_task
    running = _auto_collect_task is not None and not _auto_collect_task.done()
    now = datetime.now()
    return {
        "running": running,
        "interval_seconds": SNAPSHOT_INTERVAL,
        "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "is_trading_hours": now.weekday() < 5 and ((now.hour == 9 and now.minute >= 15) or (10 <= now.hour <= 14) or (now.hour == 15 and now.minute == 0)),
    }


@app.on_event("startup")
async def startup_event():
    """启动时执行一次采集"""
    logger.info("系统启动，执行首次采集...")
    try:
        await run_collection_cycle()
    except Exception as e:
        logger.error(f"启动采集失败: {e}")
    # 启动自动采集
    global _auto_collect_task
    _auto_collect_task = asyncio.create_task(auto_collect_loop())
    logger.info(f"自动采集已启动，间隔 {SNAPSHOT_INTERVAL} 秒")


# ============ 启动 ============
if __name__ == "__main__":
    init_db()
    print("=" * 60)
    print("  THS-Monitor v1.0 - 同花顺热榜监控与交易辅助系统")
    print("  数据源: 同花顺热榜API（实时）")
    print("  存储: SQLite本地数据库")
    print("  采集间隔: 每5分钟（交易时段自动）")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8000)
