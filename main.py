from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd
import os
import io
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright # 改用最穩定的同步 (Sync) 模組

app = FastAPI()
os.makedirs("downloads", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
def get_ui(): # 拿掉 async
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>請先建立 index.html 檔案</h1>"

@app.get("/api/analyze")
def analyze_website(url: str, limit: int = 10): # 拿掉 async
    if not url:
        raise HTTPException(status_code=400, detail="請提供網址")
    
    try:
        # 使用同步版本的 Playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            html = page.content()
            browser.close()
            
        soup = BeautifulSoup(html, 'html.parser')
        preview_data = []
        tables_found = 0
        
        try:
            html_io = io.StringIO(html)
            dfs = pd.read_html(html_io)
            if dfs:
                tables_found = len(dfs)
                df_preview = dfs[0].head(limit).fillna("") 
                preview_data = df_preview.to_dict(orient='records')
        except ValueError:
            pass

        if not preview_data:
            return {"status": "error", "message": "在此網頁找不到標準表格。"}

        return {
            "status": "success",
            "analysisResult": {
                "tablesFound": tables_found,
                "listItemsFound": len(soup.find_all('tr')),
                "paginationDetected": "下一頁" in html or ">" in html,
                "estimatedPages": "動態取得",
                "detailLinksDetected": len(soup.find_all('a')) > 0
            },
            "previewData": preview_data
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/crawl")
def crawl_website(url: str, pagination: str = 'false', startPage: int = 1, endPage: str = 'last', deepCrawl: str = 'false'): # 拿掉 async
    try:
        is_pagination = pagination.lower() == 'true'
        is_deep = deepCrawl.lower() == 'true'
        all_data = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            
            current_page = 1
            max_pages = 9 if endPage == 'last' else int(endPage) 
            if not is_pagination:
                max_pages = 1
                
            while current_page <= max_pages:
                # 等待表格與畫面載入
                try:
                    page.wait_for_selector("table", timeout=10000)
                    page.wait_for_timeout(1500) # 給予 1.5 秒緩衝時間渲染
                except Exception:
                    pass
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                try:
                    dfs = pd.read_html(io.StringIO(html))
                    if dfs:
                        df = dfs[0]
                        
                        # 【深層擷取邏輯】
                        if is_deep:
                            links = []
                            table_node = soup.find('table')
                            if table_node:
                                for tr in table_node.find_all('tr')[1:]:
                                    a_tag = tr.find('a', href=True)
                                    if a_tag:
                                        href = a_tag['href']
                                        full_url = "https://carbonfee.moenv.gov.tw" + href if href.startswith('/') else href
                                        links.append(full_url)
                                    else:
                                        links.append("無深層連結")
                                        
                                if len(links) == len(df):
                                    df['【深層明細資料連結】'] = links
                                    
                        all_data.append(df)
                except Exception:
                    pass
                
                # 【換頁邏輯】
                if is_pagination and current_page < max_pages:
                    next_btn = page.query_selector('a:has-text("下一頁"), a:has-text(">"), button.next, li.next a')
                    if next_btn:
                        next_btn.click()
                        current_page += 1
                    else:
                        break # 找不到下一頁就提早結束
                else:
                    break

            browser.close()
            
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            file_path = "downloads/result.xlsx"
            final_df.to_excel(file_path, index=False, engine='openpyxl')
            
            msg = f"成功擷取 {current_page} 頁，共 {len(final_df)} 筆資料！"
            if is_deep: msg += " (已包含深層資料連結)"
                
            return {"status": "success", "message": msg, "download_url": "/api/download"}
        else:
            return {"status": "error", "message": "無法解析表格。"}
            
    except Exception as e:
        return {"status": "error", "message": f"爬蟲發生錯誤: {str(e)}"}

@app.get("/api/download")
def download_file():
    file_path = "downloads/result.xlsx"
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename="進階爬蟲結果.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    raise HTTPException(status_code=404, detail="找不到檔案")