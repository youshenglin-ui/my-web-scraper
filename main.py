from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd
import os
import io
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright # 穩定的同步模組

app = FastAPI()
os.makedirs("downloads", exist_ok=True)

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
        print(f"[系統] 開始分析網址: {url}")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
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
                "paginationDetected": "下一頁" in html or "›" in html or "page=" in url,
                "estimatedPages": "支援自動/強制翻頁",
                "detailLinksDetected": len(soup.find_all('a')) > 0
            },
            "previewData": preview_data
        }
    except Exception as e:
        print(f"[錯誤] 分析失敗: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/api/crawl")
def crawl_website(url: str, pagination: str = 'false', startPage: int = 1, endPage: str = 'last', deepCrawl: str = 'false'):
    try:
        is_pagination = pagination.lower() == 'true'
        is_deep = deepCrawl.lower() == 'true'
        all_data = []
        
        print(f"\n========== 開始爬蟲任務 ==========")
        print(f"目標網址: {url}")
        print(f"換頁模式: {is_pagination}, 深層擷取: {is_deep}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)
            
            # 解析基礎網址 (供相對路徑的內頁連結使用)
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            current_page = 1
            max_pages = 9 if endPage == 'last' else int(endPage) 
            if not is_pagination:
                max_pages = 1
                
            while current_page <= max_pages:
                print(f"[進度] 正在擷取第 {current_page} 頁...")
                
                # 1. 等待表格與畫面載入
                try:
                    page.wait_for_selector("table", timeout=10000)
                    page.wait_for_timeout(2000) # 給予 2 秒緩衝時間確保動態資料渲染完畢
                except Exception:
                    print(f"  -> 等待表格超時，嘗試直接解析畫面。")
                    pass
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                try:
                    dfs = pd.read_html(io.StringIO(html))
                    if dfs:
                        # 若畫面中有多個表格，抓取行數最多的一個(通常是主資料表)
                        df = max(dfs, key=len)
                        print(f"  -> 第 {current_page} 頁成功取得表格，共 {len(df)} 筆主資料")
                        
                        # 2. 【真正進入內頁的深層擷取邏輯】
                        if is_deep:
                            links = []
                            table_node = soup.find('table')
                            if table_node:
                                # 抓取所有的列 (略過標題行)
                                for tr in table_node.find_all('tr')[1:]:
                                    a_tag = tr.find('a', href=True)
                                    if a_tag:
                                        href = a_tag['href']
                                        # 組合出完整的絕對路徑網址
                                        full_url = href if href.startswith('http') else base_url + href if href.startswith('/') else href
                                        links.append(full_url)
                                    else:
                                        links.append("無")
                                        
                                # 如果抓到的連結數量和表格行數對得上
                                if len(links) == len(df):
                                    df['【深層明細連結】'] = links
                                    detail_contents = []
                                    
                                    print(f"  -> 準備進入 {len(links)} 個內頁抓取詳細資料...")
                                    
                                    # 開始逐一拜訪內頁
                                    for idx, link in enumerate(links):
                                        if link.startswith("http"):
                                            try:
                                                print(f"    -> 正在抓取內頁 {idx+1}/{len(links)}: {link}")
                                                detail_page = browser.new_page()
                                                detail_page.goto(link, wait_until="networkidle", timeout=15000)
                                                detail_html = detail_page.content()
                                                
                                                # 嘗試在內頁中找尋表格
                                                try:
                                                    detail_dfs = pd.read_html(io.StringIO(detail_html))
                                                    if detail_dfs:
                                                        # 將內頁前兩個表格轉為純文字 JSON 以利存入單一 Excel 儲存格
                                                        detail_text = " | ".join([d.to_json(orient="records", force_ascii=False) for d in detail_dfs[:2]])
                                                        detail_contents.append(detail_text)
                                                        detail_page.close()
                                                        continue
                                                except ValueError:
                                                    pass
                                                    
                                                # 如果內頁沒有表格，改抓取純文字
                                                detail_soup = BeautifulSoup(detail_html, 'html.parser')
                                                for script in detail_soup(["script", "style", "nav", "footer"]):
                                                    script.extract()
                                                text = detail_soup.get_text(separator=' ', strip=True)
                                                detail_contents.append(text[:1500]) # 限制長度避免 Excel 爆掉
                                                detail_page.close()
                                            except Exception as e:
                                                print(f"    -> 內頁擷取失敗: {str(e)}")
                                                detail_contents.append(f"內頁擷取失敗")
                                                if 'detail_page' in locals() and not detail_page.is_closed():
                                                    detail_page.close()
                                        else:
                                            detail_contents.append("無有效連結")
                                            
                                    df['【內頁詳細資料 (爬蟲)】'] = detail_contents
                                    
                        all_data.append(df)
                except Exception as e:
                    print(f"  -> 解析表格失敗: {str(e)}")
                    pass
                
                # 3. 【強制無敵換頁邏輯】
                if is_pagination and current_page < max_pages:
                    current_url = page.url
                    next_clicked = False
                    
                    # 方案 A: 尋找畫面上的下一頁按鈕
                    try:
                        next_btn = page.locator('a.page-link:has-text("›"), a:has-text("下一頁"), a:has-text(">"), li.next a').first
                        if next_btn.is_visible(timeout=2000):
                            next_btn.click()
                            page.wait_for_timeout(3000)
                            current_page += 1
                            next_clicked = True
                    except Exception:
                        pass
                        
                    # 方案 B: (備用方案) 如果網址裡有 page=1，直接改網址強制跳頁
                    if not next_clicked:
                        if "page=" in current_url:
                            current_page += 1
                            new_url = re.sub(r'page=\d+', f'page={current_page}', current_url)
                            print(f"  -> 找不到按鈕，強制更換網址至下一頁: {new_url}")
                            page.goto(new_url, wait_until="networkidle", timeout=30000)
                        else:
                            print("  -> 已經無法翻頁，結束爬蟲。")
                            break # 真的無路可走才結束
                else:
                    break

            browser.close()
            
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            file_path = "downloads/result.xlsx"
            final_df.to_excel(file_path, index=False, engine='openpyxl')
            
            msg = f"成功擷取 {current_page} 頁，共 {len(final_df)} 筆主資料！"
            if is_deep: msg += " (已包含進入內頁抓取的詳細數據)"
            print(f"========== 任務完成: {msg} ==========\n")
            return {"status": "success", "message": msg, "download_url": "/api/download"}
        else:
            return {"status": "error", "message": "無法解析表格。"}
            
    except Exception as e:
        print(f"[嚴重錯誤] 爬蟲發生異常: {str(e)}")
        return {"status": "error", "message": f"爬蟲發生錯誤: {str(e)}"}

@app.get("/api/download")
def download_file():
    file_path = "downloads/result.xlsx"
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename="進階爬蟲結果.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    raise HTTPException(status_code=404, detail="找不到檔案")