from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd
import os
import io
import re
import json
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

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

@app.get("/", response_class=HTMLResponse)
def get_ui():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>請先建立 index.html 檔案</h1>"

@app.get("/api/analyze")
def analyze_website(url: str, limit: int = 10):
    if not url:
        raise HTTPException(status_code=400, detail="請提供網址")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            html = page.content()
            browser.close()
            
        soup = BeautifulSoup(html, 'html.parser')
        preview_data = []
        try:
            dfs = pd.read_html(io.StringIO(html))
            if dfs:
                df_preview = dfs[0].head(limit).fillna("") 
                preview_data = df_preview.to_dict(orient='records')
        except ValueError:
            pass

        if not preview_data:
            return {"status": "error", "message": "在此網頁找不到標準表格。"}

        return {
            "status": "success",
            "analysisResult": {
                "tablesFound": len(dfs) if 'dfs' in locals() else 0,
                "listItemsFound": len(soup.find_all('tr')),
                "paginationDetected": "page=" in url or "下一頁" in html,
                "estimatedPages": "支援自動/強制翻頁",
                "detailLinksDetected": len(soup.find_all('a')) > 0
            },
            "previewData": preview_data
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def format_pretty_text(json_data):
    text_parts = []
    for f in json_data:
        fac_name = f.get('factory_name', '')
        measures = f.get('measures', [])
        err = f.get('error', '')
        
        part = ""
        if fac_name:
            part += f"【{fac_name}】\n"
        
        if measures:
            part += "減量措施:\n"
            for m in measures:
                parts = [str(x).strip() for x in [m.get('type'), m.get('id'), m.get('name')] if x and str(x).strip()]
                tech_str = " / ".join(parts)
                part += f"  - {m.get('年度')}年度: {tech_str}\n"
        elif err:
            part += f"擷取發生異常: {err}\n"
        else:
            part += "無具體減量措施紀錄\n"
            
        text_parts.append(part.strip())
    return "\n\n".join(text_parts)

def extract_deep_info(url, browser, base_url, depth=0, factory_name="", c_no=""):
    if depth > 2: 
        return []
        
    p = browser.new_page()
    try:
        p.goto(url, wait_until="networkidle", timeout=20000)
        p.wait_for_timeout(1500)
        html = p.content()
        
        is_aggregate = False
        sub_factories = []
        try:
            dfs = pd.read_html(io.StringIO(html))
            for d in dfs:
                cols_str = [str(c) for c in d.columns]
                # 嚴格判斷：代公司子廠清單
                if '事業名稱' in d.columns and '管制編號' in d.columns and any('序號' in c for c in cols_str):
                    is_aggregate = True
                    for idx, row in d.iterrows():
                        sub_c_no = str(row['管制編號']).strip()
                        c_name = str(row['事業名稱']).strip()
                        if sub_c_no and sub_c_no != 'nan' and sub_c_no != 'None':
                            link = f"{base_url}/front/reductionpublic/list/Detail?controlNo={sub_c_no}"
                            sub_factories.append({"name": c_name, "link": link, "c_no": sub_c_no})
                    break
        except ValueError:
            pass
            
        if is_aggregate and sub_factories:
            print(f"      -> [巢狀解鎖] 自動展開 {len(sub_factories)} 間子廠資訊...")
            results = []
            for s_idx, sub in enumerate(sub_factories):
                print(f"        -> 深入擷取子廠 {s_idx+1}/{len(sub_factories)}: {sub['name']}")
                sub_results = extract_deep_info(sub['link'], browser, base_url, depth=depth+1, factory_name=sub['name'], c_no=sub['c_no'])
                results.extend(sub_results)
            p.close()
            return results
            
        else:
            # 一般廠區明細頁
            measures = []
            try:
                dfs = pd.read_html(io.StringIO(html))
                valid_dfs = []
                # 先過濾出所有包含年度與減量措施的表格
                for df in dfs:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, c)).strip('_') for c in df.columns.values]
                    cols = [str(c) for c in df.columns]
                    if any('年度' in c or '年份' in c for c in cols) and any('減量措施' in c or '技術' in c or '項目' in c for c in cols):
                        valid_dfs.append(df)
                
                # 【修正：只抓第一張有效表】保留所有合法重複項，同時避免網頁RWD隱藏表造成的假雙倍
                if valid_dfs:
                    df = valid_dfs[0]
                    cols = [str(c) for c in df.columns]
                    year_col = next((c for c in cols if '年度' in c or '年份' in c), None)
                    measure_cols = [c for c in cols if '減量措施' in c or '技術' in c or '項目' in c]
                    
                    for idx, row in df.iterrows():
                        year_val = str(row[year_col]).replace('.0', '').strip()
                        if not year_val or year_val == 'nan' or year_val == 'None': continue
                        
                        m_parts = []
                        for m_c in measure_cols:
                            val = str(row[m_c]).strip()
                            if val and val != 'nan' and val != 'None':
                                m_parts.append(val)
                        
                        if m_parts:
                            m_type = m_parts[0] if len(m_parts) > 0 else ""
                            m_name = m_parts[-1] if len(m_parts) > 1 else ""
                            if len(m_parts) == 2:
                                m_name = m_parts[1]
                                
                            measures.append({
                                '年度': year_val,
                                'type': m_type,
                                'name': m_name
                            })
            except ValueError:
                pass
                
            # 重新賦予 A, B, C, D 的編號，不再進行去重過濾，百分百原汁原味
            year_groups = {}
            for m in measures:
                y = m['年度']
                if y not in year_groups:
                    year_groups[y] = []
                year_groups[y].append(m)
                
            final_measures = []
            for y, m_list in year_groups.items():
                for i, m in enumerate(m_list):
                    id_char = chr(65 + i) if i < 26 else f"A{i-26}"
                    m['id'] = id_char
                    final_measures.append(m)
                    
            p.close()
            return [{"factory_name": factory_name, "c_no": c_no, "measures": final_measures}]
            
    except Exception as e:
        try:
            p.close()
        except:
            pass
        return [{"factory_name": factory_name, "c_no": c_no, "error": str(e)}]

@app.get("/api/crawl")
def crawl_website(url: str, pagination: str = 'true', startPage: int = 1, endPage: str = '9', deepCrawl: str = 'true', carbonAnalysis: str = 'true'):
    try:
        is_pagination = str(pagination).lower() not in ['false', '0']
        is_deep = str(deepCrawl).lower() not in ['false', '0']
        is_analysis = str(carbonAnalysis).lower() not in ['false', '0']
        
        all_data = []
        all_pivot_records = [] 
        
        max_pages = int(endPage) if endPage not in ['last', 'custom', ''] else 50
        if not is_pagination: max_pages = 1
        
        print(f"\n========== 【代公司徹底解鎖與明細保留版】開始爬蟲任務 ==========")
        print(f"目標: {url} | 抓取: {max_pages}頁")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            
            current_page = 1
            while current_page <= max_pages:
                print(f"[進度] 正在擷取第 {current_page} 頁...")
                page.wait_for_timeout(4000) 
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                try:
                    dfs = pd.read_html(io.StringIO(html))
                    if dfs:
                        df = max(dfs, key=len) 
                        if len(df) <= 1: 
                            break
                            
                        if is_deep:
                            links, detail_contents = [], []
                            main_table = None
                            for tbl in soup.find_all('table'):
                                if '事業名稱' in tbl.get_text():
                                    main_table = tbl
                                    break
                                    
                            if main_table:
                                for tr in main_table.find_all('tr')[1:]:
                                    a_tags = tr.find_all('a', href=True)
                                    found_link = "無"
                                    for a in a_tags:
                                        href = a['href']
                                        if href and "javascript" not in href.lower():
                                            found_link = href if href.startswith('http') else base_url + href if href.startswith('/') else base_url + "/" + href
                                            break
                                    links.append(found_link)
                                        
                                if len(links) > 0:
                                    links = links[:len(df)] + ["無"] * max(0, len(df) - len(links))
                                    for idx, link in enumerate(links):
                                        parent_company = str(df.iloc[idx].get('事業名稱', f'未命名工廠_{idx}')).strip()
                                        parent_c_no = str(df.iloc[idx].get('管制編號', '未提供')).strip()
                                        
                                        if link.startswith("http"):
                                            print(f"    -> 解析明細 {idx+1}/{len(links)}...")
                                            json_data = extract_deep_info(link, browser, base_url, depth=0, factory_name=parent_company, c_no=parent_c_no)
                                            
                                            if not json_data:
                                                detail_contents.append("無法取得有效明細資料")
                                                all_pivot_records.append({
                                                    '管制編號': parent_c_no,
                                                    '所屬母公司 (總表)': parent_company,
                                                    '廠區名稱': parent_company,
                                                    '年度': '無年份',
                                                    '採行技術': '無有效資料'
                                                })
                                            else:
                                                detail_contents.append(format_pretty_text(json_data))
                                                for f_data in json_data:
                                                    fac_name = str(f_data.get('factory_name') or parent_company).strip()
                                                    fac_c_no = str(f_data.get('c_no') or parent_c_no).strip()
                                                    measures = f_data.get('measures', [])
                                                    err_msg = f_data.get('error', '')
                                                    
                                                    if err_msg:
                                                        all_pivot_records.append({
                                                            '管制編號': fac_c_no,
                                                            '所屬母公司 (總表)': parent_company,
                                                            '廠區名稱': fac_name,
                                                            '年度': '無年份',
                                                            '採行技術': '擷取異常'
                                                        })
                                                    elif not measures:
                                                        all_pivot_records.append({
                                                            '管制編號': fac_c_no,
                                                            '所屬母公司 (總表)': parent_company,
                                                            '廠區名稱': fac_name,
                                                            '年度': '無年份',
                                                            '採行技術': '無具體措施'
                                                        })
                                                    else:
                                                        for m in measures:
                                                            year = str(m.get('年度', '')).strip() + "年度"
                                                            parts = [str(x).strip() for x in [m.get('type'), m.get('id'), m.get('name')] if x and str(x).strip()]
                                                            formatted_tech = " / ".join(parts)
                                                            
                                                            all_pivot_records.append({
                                                                '管制編號': fac_c_no,
                                                                '所屬母公司 (總表)': parent_company,
                                                                '廠區名稱': fac_name,
                                                                '年度': year,
                                                                '採行技術': formatted_tech
                                                            })
                                        else:
                                            detail_contents.append("無有效連結")
                                            all_pivot_records.append({
                                                '管制編號': parent_c_no,
                                                '所屬母公司 (總表)': parent_company,
                                                '廠區名稱': parent_company,
                                                '年度': '無年份',
                                                '採行技術': '無連結'
                                            })
                                            
                                    df['【內頁詳細資料 (爬蟲)】'] = detail_contents
                        all_data.append(df)
                except Exception as e:
                    print(f"  -> 表格解析異常: {str(e)}")
                
                # 雙重保險換頁
                if is_pagination and current_page < max_pages:
                    current_url = page.url
                    next_clicked = False
                    try:
                        next_btn = page.locator('a.page-link:has-text("›"), a.page-link:has-text(">"), a:has-text("下一頁"), li.next a').first
                        if next_btn.count() > 0 and next_btn.is_visible():
                            is_disabled = next_btn.evaluate('node => node.hasAttribute("disabled") || node.parentElement.classList.contains("disabled")')
                            if not is_disabled:
                                next_btn.click()
                                print("  -> [換頁] 成功點擊下一頁按鈕！等待資料載入...")
                                current_page += 1
                                next_clicked = True
                            else:
                                print("  -> [換頁] 已達末頁。")
                                break
                    except Exception:
                        pass
                        
                    if not next_clicked:
                        if "page=" in current_url:
                            current_page += 1
                            new_url = re.sub(r'page=\d+', f'page={current_page}', current_url)
                            page.goto(new_url, wait_until="domcontentloaded", timeout=30000)
                        else:
                            current_page += 1
                            separator = "&" if "?" in current_url else "?"
                            new_url = f"{current_url}{separator}page={current_page}"
                            page.goto(new_url, wait_until="domcontentloaded", timeout=30000)
                else:
                    break
            browser.close()
            
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.loc[:, ~final_df.columns.str.contains('^Unnamed')]
            
            final_df['管制編號'] = final_df['管制編號'].astype(str).str.strip()
            final_df['事業名稱'] = final_df['事業名稱'].astype(str).str.strip()
            
            file_path = "downloads/result.xlsx"
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 【Sheet 1: 原始資料】
                final_df.to_excel(writer, index=False, sheet_name='1. 原始資料')
                
                if is_analysis and all_pivot_records:
                    print("[處理] 正在繪製「6大維度商業分析報表」...")
                    rec_df = pd.DataFrame(all_pivot_records)
                    
                    def parse_category(val):
                        val = str(val).strip()
                        if val in ['無有效資料', '無具體措施', '無連結', '無', '擷取異常']: return '無'
                        return val.split(' / ')[0].strip()

                    def parse_detail(val):
                        val = str(val).strip()
                        if val in ['無有效資料', '無具體措施', '無連結', '無', '擷取異常']: return '無'
                        parts = val.split(' / ')
                        return parts[-1].strip() if len(parts) > 1 else val
                        
                    rec_df['措施大類'] = rec_df['採行技術'].apply(parse_category)
                    rec_df['具體項目名稱'] = rec_df['採行技術'].apply(parse_detail)
                    ignore_texts = ['無有效資料', '無具體措施', '無連結', '無', '擷取異常']
                    valid_df = rec_df[~rec_df['措施大類'].isin(ignore_texts) & (rec_df['年度'] != '無年份')].copy()

                    # 【Sheet 2: 各廠年度措施總表】 (修正：移除Left Join，保留所有真實子廠，不再顯示大量未提及)
                    grouped = rec_df.groupby(['管制編號', '所屬母公司 (總表)', '廠區名稱', '年度'])['採行技術'].apply(
                        lambda x: '\n'.join([item for item in x if item not in ignore_texts and item])
                    ).reset_index()
                    
                    pivot_df = grouped.pivot(index=['管制編號', '所屬母公司 (總表)', '廠區名稱'], columns='年度', values='採行技術')
                    if '無年份' in pivot_df.columns:
                        pivot_df = pivot_df.drop(columns=['無年份'])
                    
                    # 使用 pivot_df 本身作為基底，保證不遺失任何一家子廠
                    final_pivot = pivot_df.replace('', None).fillna('未提及').reset_index()
                    final_pivot.columns.name = None
                    final_pivot = final_pivot.sort_values(by=['所屬母公司 (總表)', '廠區名稱'])
                    
                    final_pivot.to_excel(writer, index=False, sheet_name='2. 廠區年度措施矩陣總表')

                    # 【Sheet 3: 年度措施採用比例】
                    if not valid_df.empty:
                        yearly_factory_count = valid_df.groupby('年度')['廠區名稱'].nunique()
                        measure_trend = valid_df.groupby(['年度', '措施大類'])['廠區名稱'].nunique().reset_index(name='採用廠區數量')
                        measure_trend['該年度總提報廠區數'] = measure_trend['年度'].map(yearly_factory_count)
                        measure_trend['採用廠商比例'] = (measure_trend['採用廠區數量'] / measure_trend['該年度總提報廠區數'] * 100).round(1).astype(str) + '%'
                        measure_trend = measure_trend.sort_values(by=['年度', '採用廠區數量'], ascending=[True, False])
                    else:
                        measure_trend = pd.DataFrame(columns=['年度', '措施大類', '採用廠區數量', '該年度總提報廠區數', '採用廠商比例'])
                    measure_trend.to_excel(writer, index=False, sheet_name='3. 年度措施採用比例')

                    # 【Sheet 4: 措施涵蓋項目分析】
                    if not valid_df.empty:
                        item_analysis = valid_df.groupby(['措施大類', '具體項目名稱']).agg(
                            被採納總次數=('具體項目名稱', 'count'),
                            採用廠區數量=('廠區名稱', 'nunique'),
                            代表性廠區=('廠區名稱', lambda x: '、'.join(list(x.unique())[:3]) + ('...' if len(x.unique())>3 else ''))
                        ).reset_index()
                        item_analysis = item_analysis.sort_values(by=['措施大類', '被採納總次數'], ascending=[True, False])
                    else:
                        item_analysis = pd.DataFrame(columns=['措施大類', '具體項目名稱', '被採納總次數', '採用廠區數量', '代表性廠區'])
                    item_analysis.to_excel(writer, index=False, sheet_name='4. 措施涵蓋細項分析')

                    # 【Sheet 5: 管制編號(區域)趨勢分析】
                    if not valid_df.empty:
                        valid_df['管制編號字首'] = valid_df['管制編號'].astype(str).str[0].str.upper()
                        valid_df['所屬區域(依編號推估)'] = valid_df['管制編號字首'].map(COUNTY_MAP).fillna('未知區域')
                        
                        region_trend = valid_df.groupby(['管制編號字首', '所屬區域(依編號推估)', '年度'])['廠區名稱'].nunique().reset_index(name='投入廠區數')
                        region_pivot = region_trend.pivot(index=['管制編號字首', '所屬區域(依編號推估)'], columns='年度', values='投入廠區數').fillna(0).astype(int)
                        
                        measures_count = valid_df.groupby(['管制編號字首'])['採行技術'].count().to_dict()
                        region_pivot['提報措施總筆數'] = region_pivot.index.get_level_values(0).map(measures_count).fillna(0).astype(int)
                        
                        region_pivot['總計投入廠區'] = region_pivot.drop(columns=['提報措施總筆數'], errors='ignore').sum(axis=1)
                        region_pivot = region_pivot.reset_index().sort_values(by='提報措施總筆數', ascending=False)
                        region_pivot.columns.name = None
                    else:
                        region_pivot = pd.DataFrame(columns=['管制編號字首', '所屬區域(依編號推估)', '總計投入廠區', '提報措施總筆數'])
                    region_pivot.to_excel(writer, index=False, sheet_name='5. 區域與管制編號趨勢分析')

                    # 【Sheet 6: 企業減碳積極度排行榜】
                    if not valid_df.empty:
                        rank_df = valid_df.groupby(['所屬母公司 (總表)', '廠區名稱', '管制編號']).agg(
                            總措施數量=('採行技術', 'count'),
                            涵蓋類別數=('措施大類', 'nunique'),
                            持續投入年數=('年度', 'nunique')
                        ).reset_index()
                        rank_df = rank_df.sort_values(by=['總措施數量', '涵蓋類別數', '持續投入年數'], ascending=[False, False, False])
                        rank_df['積極度排名'] = rank_df['總措施數量'].rank(method='min', ascending=False).astype(int)
                        rank_df = rank_df[['積極度排名', '所屬母公司 (總表)', '廠區名稱', '管制編號', '總措施數量', '涵蓋類別數', '持續投入年數']]
                    else:
                        rank_df = pd.DataFrame()
                    rank_df.to_excel(writer, index=False, sheet_name='6. 企業減碳積極度排行榜')
            
            msg = f"成功擷取共 {len(final_df)} 筆資料！ (已產生 6 大維度商業分析報表)"
            print(f"========== 任務完成 ==========\n")
            return {"status": "success", "message": msg, "download_url": "/api/download"}
        else:
            return {"status": "error", "message": "無法解析表格。"}
            
    except Exception as e:
        return {"status": "error", "message": f"爬蟲發生錯誤: {str(e)}"}

@app.get("/api/download")
def download_file():
    if os.path.exists("downloads/result.xlsx"):
        return FileResponse("downloads/result.xlsx", filename="環境部碳減量綜合分析報告.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    raise HTTPException(status_code=404, detail="找不到檔案")