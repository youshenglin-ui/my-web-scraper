from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pandas as pd
import os
import io
import re
import uuid
import asyncio
import sqlite3
import datetime
import json
import requests
from typing import Dict, Any
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

app = FastAPI()
os.makedirs("downloads", exist_ok=True)

# 台灣環保署管制編號首字母對應縣市字典
COUNTY_MAP = {
    'A': '臺北市', 'B': '臺中市', 'C': '基隆市', 'D': '臺南市',
    'E': '高雄市', 'F': '新北市', 'G': '宜蘭縣', 'H': '桃園市',
    'I': '嘉義市', 'J': '新竹縣', 'K': '苗栗縣', 'L': '臺中市(原中縣)',
    'M': '南投縣', 'N': '彰化縣', 'O': '新竹市', 'P': '雲林縣',
    'Q': '嘉義縣', 'R': '臺南市(原南縣)', 'S': '高雄市(原高縣)', 'T': '屏東縣',
    'U': '花蓮縣', 'V': '臺東縣', 'W': '金門縣', 'X': '澎湖縣',
    'Y': '陽明山', 'Z': '連江縣'
}

# ----------------- 資料庫與升級腳本 -----------------
def init_db():
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS schedules 
                 (id TEXT PRIMARY KEY, url TEXT, config TEXT, cron_time TEXT, created_at DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS task_logs 
                 (id TEXT PRIMARY KEY, schedule_id TEXT, task_type TEXT, status TEXT, message TEXT, file_name TEXT, executed_at DATETIME)''')
    
    # 升級欄位
    try: c.execute("ALTER TABLE schedules ADD COLUMN webhook_url TEXT")
    except sqlite3.OperationalError: pass
    try: c.execute("ALTER TABLE schedules ADD COLUMN custom_rule TEXT")
    except sqlite3.OperationalError: pass
    
    conn.commit()
    conn.close()

init_db()
scheduler = AsyncIOScheduler()
TASKS: Dict[str, Dict[str, Any]] = {}

# ----------------- Webhook 通知模組 (取代 LINE Notify) -----------------
def send_webhook_notify(webhook_url: str, message: str):
    if not webhook_url or not webhook_url.strip(): return
    try:
        # 預設使用 Discord 的 JSON 格式 (content)，若為 Slack 則自動相容 (text)
        payload = {"content": f"🤖 [爬蟲系統通知]\n{message}"}
        if "hooks.slack.com" in webhook_url:
            payload = {"text": f"🤖 [爬蟲系統通知]\n{message}"}
            
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        print(f"Webhook 通知發送失敗: {e}")

# ----------------- 排程初始化 -----------------
@app.on_event("startup")
async def start_scheduler():
    scheduler.start()
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    c.execute("SELECT id, url, config, cron_time, webhook_url, custom_rule FROM schedules")
    rows = c.fetchall()
    conn.close()
    
    for row in rows:
        schedule_id, url, config, cron_time, webhook_url, custom_rule = row
        hour, minute = cron_time.split(':')
        scheduler.add_job(
            run_scheduled_crawl, CronTrigger(hour=hour, minute=minute), id=schedule_id,
            args=[schedule_id, url, config, webhook_url, custom_rule], replace_existing=True
        )
    print(f"✅ 排程器已啟動，共載入 {len(rows)} 個自動任務。")

@app.get("/", response_class=HTMLResponse)
async def get_ui():
    try:
        with open("index.html", "r", encoding="utf-8") as f: return f.read()
    except FileNotFoundError: return "<h1>請先建立 index.html 檔案</h1>"

@app.get("/api/analyze")
async def analyze_website(url: str, limit: int = 10):
    if not url: raise HTTPException(status_code=400, detail="請提供網址")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = await browser.new_page()
            await page.goto(url, wait_until="networkidle", timeout=30000)
            html = await page.content()
            await browser.close()
            
        soup = BeautifulSoup(html, 'html.parser')
        preview_data = []
        try:
            dfs = pd.read_html(io.StringIO(html))
            if dfs:
                df_preview = dfs[0].head(limit).fillna("") 
                preview_data = df_preview.to_dict(orient='records')
        except ValueError: pass

        if not preview_data: return {"status": "error", "message": "找不到標準表格。"}
        return {
            "status": "success",
            "analysisResult": { "tablesFound": len(dfs) if 'dfs' in locals() else 0, "listItemsFound": len(soup.find_all('tr')) },
            "previewData": preview_data
        }
    except Exception as e: return {"status": "error", "message": str(e)}

# ==============================================================================
# 核心模組 A：環保署碳費專屬深層爬蟲引擎 (完整保留無刪減)
# ==============================================================================
def format_pretty_text(json_data):
    text_parts = []
    for f in json_data:
        fac_name = f.get('factory_name', '')
        measures = f.get('measures', [])
        err = f.get('error', '')
        part = ""
        if fac_name: part += f"【{fac_name}】\n"
        if measures:
            part += "減量措施:\n"
            for m in measures:
                parts = [str(x).strip() for x in [m.get('type'), m.get('id'), m.get('name')] if x and str(x).strip()]
                tech_str = " / ".join(parts)
                part += f"  - {m.get('年度')}年度: {tech_str}\n"
        elif err: part += f"擷取發生異常: {err}\n"
        else: part += "無具體減量措施紀錄\n"
        text_parts.append(part.strip())
    return "\n\n".join(text_parts)

async def extract_deep_info_async(url, browser, base_url, depth=0, factory_name="", c_no=""):
    if depth > 2: return []
    p = await browser.new_page()
    try:
        await p.goto(url, wait_until="networkidle", timeout=20000)
        await p.wait_for_timeout(1500)
        html = await p.content()
        is_aggregate = False
        sub_factories = []
        try:
            dfs = pd.read_html(io.StringIO(html))
            for d in dfs:
                cols_str = [str(c) for c in d.columns]
                if '事業名稱' in d.columns and '管制編號' in d.columns and any('序號' in c for c in cols_str):
                    is_aggregate = True
                    for idx, row in d.iterrows():
                        sub_c_no = str(row['管制編號']).strip()
                        c_name = str(row['事業名稱']).strip()
                        if sub_c_no and sub_c_no != 'nan' and sub_c_no != 'None':
                            link = f"{base_url}/front/reductionpublic/list/Detail?controlNo={sub_c_no}"
                            sub_factories.append({"name": c_name, "link": link, "c_no": sub_c_no})
                    break
        except ValueError: pass
            
        if is_aggregate and sub_factories:
            results = []
            for sub in sub_factories:
                sub_results = await extract_deep_info_async(sub['link'], browser, base_url, depth=depth+1, factory_name=sub['name'], c_no=sub['c_no'])
                results.extend(sub_results)
            await p.close()
            return results
        else:
            measures = []
            try:
                dfs = pd.read_html(io.StringIO(html))
                valid_dfs = []
                for df in dfs:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, c)).strip('_') for c in df.columns.values]
                    cols = [str(c) for c in df.columns]
                    if any('年度' in c or '年份' in c for c in cols) and any('減量措施' in c or '技術' in c or '項目' in c for c in cols):
                        valid_dfs.append(df)
                if valid_dfs:
                    df = valid_dfs[0]
                    cols = [str(c) for c in df.columns]
                    year_col = next((c for c in cols if '年度' in c or '年份' in c), None)
                    measure_cols = [c for c in cols if '減量措施' in c or '技術' in c or '項目' in c]
                    for idx, row in df.iterrows():
                        year_val = str(row[year_col]).replace('.0', '').strip()
                        if not year_val or year_val == 'nan' or year_val == 'None': continue
                        m_parts = [str(row[m_c]).strip() for m_c in measure_cols if str(row[m_c]).strip() not in ['nan', 'None', '']]
                        if m_parts:
                            m_type = m_parts[0] if len(m_parts) > 0 else ""
                            m_name = m_parts[-1] if len(m_parts) > 1 else ""
                            if len(m_parts) == 2: m_name = m_parts[1]
                            measures.append({'年度': year_val, 'type': m_type, 'name': m_name})
            except ValueError: pass
                
            year_groups = {}
            for m in measures:
                y = m['年度']
                if y not in year_groups: year_groups[y] = []
                year_groups[y].append(m)
            final_measures = []
            for y, m_list in year_groups.items():
                for i, m in enumerate(m_list):
                    m['id'] = chr(65 + i) if i < 26 else f"A{i-26}"
                    final_measures.append(m)
            await p.close()
            return [{"factory_name": factory_name, "c_no": c_no, "measures": final_measures}]
    except Exception as e:
        try: await p.close()
        except: pass
        return [{"factory_name": factory_name, "c_no": c_no, "error": str(e)}]

async def core_crawler_engine(url: str, max_pages: int, is_pagination: bool, is_deep: bool, task_id: str = None):
    """環境部專屬：深層爬蟲主程式"""
    all_data = []
    all_pivot_records = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=30000)
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        current_page = 1
        while current_page <= max_pages:
            if task_id and task_id in TASKS: 
                TASKS[task_id]['message'] = f"擷取第 {current_page} 頁..."
                TASKS[task_id]['progress'] = int(10 + (current_page/max_pages)*70)
            
            await page.wait_for_timeout(3000)
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            try:
                dfs = pd.read_html(io.StringIO(html))
                if dfs:
                    df = max(dfs, key=len)
                    if len(df) <= 1: break
                    if is_deep:
                        links, detail_contents = [], []
                        main_table = next((tbl for tbl in soup.find_all('table') if '事業名稱' in tbl.get_text()), None)
                        if main_table:
                            for tr in main_table.find_all('tr')[1:]:
                                found_link = "無"
                                for a in tr.find_all('a', href=True):
                                    href = a['href']
                                    if href and "javascript" not in href.lower():
                                        found_link = href if href.startswith('http') else (base_url + href if href.startswith('/') else base_url + "/" + href)
                                        break
                                links.append(found_link)
                            
                            links = links[:len(df)] + ["無"] * max(0, len(df) - len(links))
                            for idx, link in enumerate(links):
                                parent_company = str(df.iloc[idx].get('事業名稱', f'未命名工廠_{idx}')).strip()
                                parent_c_no = str(df.iloc[idx].get('管制編號', '未提供')).strip()
                                
                                if link.startswith("http"):
                                    if task_id and task_id in TASKS: TASKS[task_id]['message'] = f"解析深層資料: {parent_company}..."
                                    json_data = await extract_deep_info_async(link, browser, base_url, factory_name=parent_company, c_no=parent_c_no)
                                    if not json_data:
                                        detail_contents.append("無法取得有效明細資料")
                                        all_pivot_records.append({'管制編號': parent_c_no, '所屬母公司 (總表)': parent_company, '廠區名稱': parent_company, '年度': '無年份', '採行技術': '無有效資料'})
                                    else:
                                        detail_contents.append(format_pretty_text(json_data))
                                        for f_data in json_data:
                                            fac_name = str(f_data.get('factory_name') or parent_company).strip()
                                            fac_c_no = str(f_data.get('c_no') or parent_c_no).strip()
                                            for m in f_data.get('measures', []):
                                                year = str(m.get('年度', '')).strip() + "年度"
                                                parts = [str(x).strip() for x in [m.get('type'), m.get('id'), m.get('name')] if x and str(x).strip()]
                                                all_pivot_records.append({'管制編號': fac_c_no, '所屬母公司 (總表)': parent_company, '廠區名稱': fac_name, '年度': year, '採行技術': " / ".join(parts)})
                                else:
                                    detail_contents.append("無有效連結")
                            df['【內頁詳細資料】'] = detail_contents
                    all_data.append(df)
            except Exception: pass
            
            if is_pagination and current_page < max_pages:
                try:
                    next_btn = page.locator('a.page-link:has-text("›"), a.page-link:has-text(">"), a:has-text("下一頁"), li.next a').first
                    if await next_btn.count() > 0 and not await next_btn.evaluate('node => node.hasAttribute("disabled")'):
                        await next_btn.click()
                        current_page += 1
                    else: break
                except Exception: break
            else: break
        await browser.close()
    return all_data, all_pivot_records

def generate_excel_report(all_data, all_pivot_records, is_analysis, file_path):
    """環境部專屬：產生 6 大維度商業分析報表"""
    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.loc[:, ~final_df.columns.str.contains('^Unnamed')]
    if '管制編號' in final_df.columns: final_df['管制編號'] = final_df['管制編號'].astype(str).str.strip()
    if '事業名稱' in final_df.columns: final_df['事業名稱'] = final_df['事業名稱'].astype(str).str.strip()
    
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name='1. 原始資料')
        
        if is_analysis and all_pivot_records:
            rec_df = pd.DataFrame(all_pivot_records)
            def parse_category(val):
                val = str(val).strip()
                return '無' if val in ['無有效資料', '無具體措施', '無連結', '無', '擷取異常'] else val.split(' / ')[0].strip()
            def parse_detail(val):
                val = str(val).strip()
                if val in ['無有效資料', '無具體措施', '無連結', '無', '擷取異常']: return '無'
                parts = val.split(' / ')
                return parts[-1].strip() if len(parts) > 1 else val
                
            rec_df['措施大類'] = rec_df['採行技術'].apply(parse_category)
            rec_df['具體項目名稱'] = rec_df['採行技術'].apply(parse_detail)
            ignore_texts = ['無有效資料', '無具體措施', '無連結', '無', '擷取異常']
            valid_df = rec_df[~rec_df['措施大類'].isin(ignore_texts) & (rec_df['年度'] != '無年份')].copy()

            # 2. 廠區年度措施矩陣
            grouped = rec_df.groupby(['管制編號', '所屬母公司 (總表)', '廠區名稱', '年度'])['採行技術'].apply(
                lambda x: '\n'.join([item for item in x if item not in ignore_texts and item])
            ).reset_index()
            pivot_df = grouped.pivot(index=['管制編號', '所屬母公司 (總表)', '廠區名稱'], columns='年度', values='採行技術')
            if '無年份' in pivot_df.columns: pivot_df = pivot_df.drop(columns=['無年份'])
            final_pivot = pivot_df.replace('', None).fillna('未提及').reset_index().sort_values(by=['所屬母公司 (總表)', '廠區名稱'])
            final_pivot.to_excel(writer, index=False, sheet_name='2. 廠區年度措施矩陣總表')

            # 3. 年度措施採用比例
            if not valid_df.empty:
                yearly_factory_count = valid_df.groupby('年度')['廠區名稱'].nunique()
                measure_trend = valid_df.groupby(['年度', '措施大類'])['廠區名稱'].nunique().reset_index(name='採用廠區數量')
                measure_trend['該年度總提報廠區數'] = measure_trend['年度'].map(yearly_factory_count)
                measure_trend['採用廠商比例'] = (measure_trend['採用廠區數量'] / measure_trend['該年度總提報廠區數'] * 100).round(1).astype(str) + '%'
                measure_trend = measure_trend.sort_values(by=['年度', '採用廠區數量'], ascending=[True, False])
                measure_trend.to_excel(writer, index=False, sheet_name='3. 年度措施採用比例')

            # 4. 措施涵蓋細項分析
            if not valid_df.empty:
                item_analysis = valid_df.groupby(['措施大類', '具體項目名稱']).agg(
                    被採納總次數=('具體項目名稱', 'count'),
                    採用廠區數量=('廠區名稱', 'nunique'),
                    代表性廠區=('廠區名稱', lambda x: '、'.join(list(x.unique())[:3]) + ('...' if len(x.unique())>3 else ''))
                ).reset_index().sort_values(by=['措施大類', '被採納總次數'], ascending=[True, False])
                item_analysis.to_excel(writer, index=False, sheet_name='4. 措施涵蓋細項分析')

            # 5. 區域與管制編號趨勢
            if not valid_df.empty:
                valid_df['管制編號字首'] = valid_df['管制編號'].astype(str).str[0].str.upper()
                valid_df['所屬區域'] = valid_df['管制編號字首'].map(COUNTY_MAP).fillna('未知區域')
                region_trend = valid_df.groupby(['管制編號字首', '所屬區域', '年度'])['廠區名稱'].nunique().reset_index(name='投入廠區數')
                region_pivot = region_trend.pivot(index=['管制編號字首', '所屬區域'], columns='年度', values='投入廠區數').fillna(0).astype(int)
                measures_count = valid_df.groupby(['管制編號字首'])['採行技術'].count().to_dict()
                region_pivot['提報措施總筆數'] = region_pivot.index.get_level_values(0).map(measures_count).fillna(0).astype(int)
                region_pivot['總計投入廠區'] = region_pivot.drop(columns=['提報措施總筆數'], errors='ignore').sum(axis=1)
                region_pivot.reset_index().sort_values(by='提報措施總筆數', ascending=False).to_excel(writer, index=False, sheet_name='5. 區域趨勢分析')

            # 6. 企業減碳積極度排行榜
            if not valid_df.empty:
                rank_df = valid_df.groupby(['所屬母公司 (總表)', '廠區名稱', '管制編號']).agg(
                    總措施數量=('採行技術', 'count'), 涵蓋類別數=('措施大類', 'nunique'), 持續投入年數=('年度', 'nunique')
                ).reset_index().sort_values(by=['總措施數量', '涵蓋類別數', '持續投入年數'], ascending=[False, False, False])
                rank_df['積極度排名'] = rank_df['總措施數量'].rank(method='min', ascending=False).astype(int)
                rank_df[['積極度排名', '所屬母公司 (總表)', '廠區名稱', '管制編號', '總措施數量', '涵蓋類別數', '持續投入年數']].to_excel(writer, index=False, sheet_name='6. 企業積極度排行榜')
    return len(final_df)


# ==============================================================================
# 核心模組 B：通用網站爬蟲引擎 (支援自訂 CSS 選擇器)
# ==============================================================================
async def universal_crawler_engine(url: str, max_pages: int, custom_rule_str: str, task_id: str = None):
    """根據 JSON 規則進行通用爬取"""
    all_data = []
    rule = {}
    try:
        if custom_rule_str and custom_rule_str.strip() not in ["", "{}"]: 
            rule = json.loads(custom_rule_str)
    except Exception as e:
        print(f"JSON 規則解析失敗: {e}")

    table_selector = rule.get("tableSelector", "table")
    next_btn_selector = rule.get("paginationSelector", 'a:has-text("下一頁"), li.next a, a.next, .next-page')
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=30000)
        
        current_page = 1
        while current_page <= max_pages:
            if task_id and task_id in TASKS: 
                TASKS[task_id]['message'] = f"擷取第 {current_page} 頁 (通用模式)..."
                TASKS[task_id]['progress'] = int(10 + (current_page/max_pages)*70)

            await page.wait_for_timeout(3000)
            html = await page.content()
            
            try:
                soup = BeautifulSoup(html, 'html.parser')
                target_html = str(soup.select_one(table_selector)) if soup.select_one(table_selector) else html
                dfs = pd.read_html(io.StringIO(target_html))
                if dfs:
                    df = max(dfs, key=len)
                    if len(df) > 0: all_data.append(df)
            except Exception as e:
                print(f"通用解析表格異常: {e}")
            
            if current_page < max_pages:
                try:
                    next_btn = page.locator(next_btn_selector).first
                    if await next_btn.count() > 0 and not await next_btn.evaluate('node => node.hasAttribute("disabled")'):
                        await next_btn.click()
                        current_page += 1
                    else: break
                except Exception: break
            else: break
        await browser.close()
    return all_data

# ----------------- 單次任務 API -----------------
async def run_single_task(task_id: str, url: str, pagination: str, endPage: str, deepCrawl: str, carbonAnalysis: str, customRule: str):
    try:
        is_pagination = str(pagination).lower() not in ['false', '0']
        is_deep = str(deepCrawl).lower() not in ['false', '0']
        is_analysis = str(carbonAnalysis).lower() not in ['false', '0']
        max_pages = int(endPage) if endPage not in ['last', 'custom', ''] else 50
        if not is_pagination: max_pages = 1
        
        TASKS[task_id]['progress'] = 10
        
        # 智慧判斷：如果自訂規則不是空的，就用「通用引擎」；如果是空的，就用「環境部引擎」
        is_custom = customRule and customRule.strip() not in ["", "{}"]
        
        if is_custom:
            TASKS[task_id]['message'] = "已套用通用規則，啟動通用引擎..."
            all_data = await universal_crawler_engine(url, max_pages, customRule, task_id)
            total_rows = 0
            if all_data:
                TASKS[task_id]['progress'] = 85
                final_df = pd.concat(all_data, ignore_index=True)
                total_rows = len(final_df)
                file_name = f"single_{task_id[:8]}.xlsx"
                file_path = f"downloads/{file_name}"
                final_df.to_excel(file_path, index=False)
        else:
            TASKS[task_id]['message'] = "偵測無自訂規則，啟動環境部碳費深層引擎..."
            all_data, all_pivot_records = await core_crawler_engine(url, max_pages, is_pagination, is_deep, task_id)
            total_rows = 0
            if all_data:
                TASKS[task_id]['progress'] = 85
                TASKS[task_id]['message'] = "擷取完成，正在產生 6 大維度 Excel 報表..."
                file_name = f"single_{task_id[:8]}.xlsx"
                file_path = f"downloads/{file_name}"
                total_rows = generate_excel_report(all_data, all_pivot_records, is_analysis, file_path)

        if all_data:
            conn = sqlite3.connect('scraper.db')
            c = conn.cursor()
            c.execute("INSERT INTO task_logs (id, schedule_id, task_type, status, message, file_name, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (task_id, 'none', 'single', 'success', f'抓取成功，共 {total_rows} 筆', file_name, datetime.datetime.now()))
            conn.commit()
            conn.close()

            TASKS[task_id]['progress'] = 100
            TASKS[task_id]['status'] = "success"
            TASKS[task_id]['message'] = f"擷取完成！共 {total_rows} 筆"
            TASKS[task_id]['download_url'] = f"/api/download/{file_name}"
        else:
            raise Exception("無法解析表格")
    except Exception as e:
        TASKS[task_id]['status'] = "error"
        TASKS[task_id]['message'] = f"錯誤: {str(e)}"

@app.get("/api/crawl")
async def start_crawl_api(url: str, pagination: str = 'true', endPage: str = '9', deepCrawl: str = 'true', carbonAnalysis: str = 'true', customRule: str = '{}'):
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {"status": "processing", "progress": 0, "message": "啟動中...", "download_url": ""}
    asyncio.create_task(run_single_task(task_id, url, pagination, endPage, deepCrawl, carbonAnalysis, customRule))
    return {"status": "success", "task_id": task_id}

@app.get("/api/crawl/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id not in TASKS: raise HTTPException(status_code=404, detail="找不到任務")
    return TASKS[task_id]

# ----------------- 排程中心 API -----------------
async def run_scheduled_crawl(schedule_id: str, url: str, config_str: str, webhook_url: str, custom_rule: str):
    print(f"⏰ 排程執行: {schedule_id}")
    log_id = str(uuid.uuid4())
    try:
        is_custom = custom_rule and custom_rule.strip() not in ["", "{}"]
        
        if is_custom:
            all_data = await universal_crawler_engine(url, max_pages=5, custom_rule_str=custom_rule)
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                total_rows = len(final_df)
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                file_name = f"auto_{schedule_id[:6]}_{timestamp}.xlsx"
                file_path = f"downloads/{file_name}"
                final_df.to_excel(file_path, index=False)
            else:
                raise Exception("無效的資料內容 (通用)")
        else:
            # 使用環境部引擎
            config = json.loads(config_str) if config_str else {}
            is_deep = config.get('deepCrawl', True)
            is_analysis = config.get('carbonAnalysis', True)
            all_data, all_pivot_records = await core_crawler_engine(url, max_pages=5, is_pagination=True, is_deep=is_deep)
            if all_data:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                file_name = f"auto_{schedule_id[:6]}_{timestamp}.xlsx"
                file_path = f"downloads/{file_name}"
                total_rows = generate_excel_report(all_data, all_pivot_records, is_analysis, file_path)
            else:
                raise Exception("無效的資料內容 (環保署)")
        
        msg = f"自動排程成功，共擷取 {total_rows} 筆資料。\n目標: {url}"
        conn = sqlite3.connect('scraper.db')
        c = conn.cursor()
        c.execute("INSERT INTO task_logs (id, schedule_id, task_type, status, message, file_name, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (log_id, schedule_id, 'auto', 'success', msg, file_name, datetime.datetime.now()))
        conn.commit()
        conn.close()
        
        send_webhook_notify(webhook_url, f"✅ 任務成功！\n{msg}\n檔案已生成：{file_name}")
        print(f"✅ 排程 {schedule_id} 完畢！")
    except Exception as e:
        error_msg = f"執行失敗: {str(e)}"
        conn = sqlite3.connect('scraper.db')
        c = conn.cursor()
        c.execute("INSERT INTO task_logs (id, schedule_id, task_type, status, message, file_name, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (log_id, schedule_id, 'auto', 'error', error_msg, '', datetime.datetime.now()))
        conn.commit()
        conn.close()
        
        send_webhook_notify(webhook_url, f"❌ 任務失敗！\n目標: {url}\n原因: {error_msg}")
        print(f"❌ 排程 {schedule_id} 失敗: {str(e)}")

@app.post("/api/schedule/add")
async def add_schedule(data: dict):
    url = data.get('url')
    cron_time = data.get('cron_time', '08:00')
    webhook_url = data.get('webhook_url', '')
    custom_rule = data.get('custom_rule', '{}')
    
    if not url: raise HTTPException(status_code=400, detail="請提供網址")
    
    schedule_id = str(uuid.uuid4())
    hour, minute = cron_time.split(':')
    config_str = json.dumps({"deepCrawl": True, "carbonAnalysis": True})
    
    scheduler.add_job(
        run_scheduled_crawl, CronTrigger(hour=hour, minute=minute), id=schedule_id,
        args=[schedule_id, url, config_str, webhook_url, custom_rule]
    )
    
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    c.execute("INSERT INTO schedules (id, url, config, cron_time, webhook_url, custom_rule, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
              (schedule_id, url, config_str, cron_time, webhook_url, custom_rule, datetime.datetime.now()))
    conn.commit()
    conn.close()
    return {"status": "success", "message": "排程已建立！"}

@app.get("/api/schedules")
async def get_schedules():
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    c.execute("SELECT id, url, cron_time, webhook_url, created_at FROM schedules ORDER BY created_at DESC")
    schedules = [{"id": r[0], "url": r[1], "cron_time": r[2], "webhook_url": r[3], "created_at": r[4]} for r in c.fetchall()]
    
    c.execute("SELECT id, schedule_id, task_type, status, message, file_name, executed_at FROM task_logs ORDER BY executed_at DESC LIMIT 20")
    logs = [{"id": r[0], "schedule_id": r[1], "task_type": r[2], "status": r[3], "message": r[4], "file_name": r[5], "executed_at": r[6]} for r in c.fetchall()]
    conn.close()
    return {"status": "success", "schedules": schedules, "logs": logs}

@app.delete("/api/schedule/{schedule_id}")
async def delete_schedule(schedule_id: str):
    try: scheduler.remove_job(schedule_id)
    except: pass
    conn = sqlite3.connect('scraper.db')
    c = conn.cursor()
    c.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    conn.commit()
    conn.close()
    return {"status": "success", "message": "刪除成功"}

@app.get("/api/download/{file_name}")
async def download_file(file_name: str):
    file_path = f"downloads/{file_name}"
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=file_name, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    raise HTTPException(status_code=404, detail="找不到檔案")