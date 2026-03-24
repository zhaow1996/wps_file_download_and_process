# ==============================================
# 【全局配置区 - 所有需要修改的设置都在这里】
# ==============================================
# 1. 爬虫基础配置
URLS = [
    'https://www.kdocs.cn/l/cibolyZ87S0l',  # 原料一科月报表
    'https://www.kdocs.cn/l/cmIhGPuoC7mD'  # 原料二科月报表
]
CHROMEDRIVER_PATH = r"C:\Users\Administrator\Desktop\report\chromedriver.exe"
DOWNLOAD_FOLDER = r"C:\Users\Administrator\Downloads"  # 下载目录
SCHEDULE_TIME = "08:45"  # 每天定时执行时间

# 2. Excel合并配置
FILE1_NAME = "原料一科采购日报表.xlsx"
FILE2_NAME = "原料二科采购日报表.xlsx"
OUTPUT_FILENAME = "mergedexcel.xlsx"

# 3. 钉钉机器人配置
DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e551d587af7d30cb4fd9a92bd52cd886450d84dd0a1ef61922efc7cf81cb297b"
DINGTALK_SECRET = "SECba5744b2a1f21ce654ea60ecbef4c09af37437f6a73fb628f6028162e4197850"
AT_MOBILES = ["18504272160"]
AT_ALL = False

# 4. Azure 存储配置
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=zwlalalala;AccountKey=43h4Kbt6xg3KlXzsfYUsAR90CQAAw2uL8CETaMWBRaaRcFoNaLGQNhJXyundxdIH6Ud4Gn7Ncrme+ASts9Mttg==;EndpointSuffix=core.windows.net"
AZURE_CONTAINER_NAME = "excel"

# 固定列名配置（一般无需修改）
INVENTORY_COLUMNS = [
    "物料编码", "物料名称", "物料属性", "存储容量", "所属基地",
    "所属厂区", "无法正常出库量（kg）", "上周末库存（kg）",
    "实时库存（除去无法正常出库量kg）", "仓库最大存放量（kg）",
    "入库 星期一", "出库 星期一", "入库 星期二", "出库 星期二",
    "入库 星期三", "出库 星期三", "入库 星期四", "出库 星期四",
    "入库 星期五", "出库 星期五", "入库 星期六", "出库 星期六",
    "入库 星期日", "出库 星期日", "本周总计入库(kg)", "本周总计出库(kg)"
]
PURCHASE_COLUMNS = [
    "物料编码", "物料名称", "所属厂区", "采购负责人",
    "采购策略审批人", "协议类型", "支付方式", "供应商",
    "合同价格", "当日价格", "签约合同量", "合同已执行量",
    "待执行量", "关注状态", "价格及供应趋势判断", "所属基地"
]

# ==============================================
# 【依赖导入区】
# ==============================================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import os
import schedule
import threading
from datetime import datetime
import logging
import shutil
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import warnings
import requests
import hmac
import hashlib
import base64
import urllib.parse
from azure.storage.blob import BlobServiceClient
import configparser
import sys

warnings.filterwarnings('ignore')

# ==============================================
# 日志配置
# ==============================================
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('kdocs_auto_process.log', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = setup_logging()

# ==============================================
# 清空下载目录（确保先清空）
# ==============================================
def clear_download_folder(folder_path):
    try:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            return
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                logger.warning(f"删除失败 {item_path}: {str(e)}")
        logger.info(f"✅ 下载目录已清空: {folder_path}")
    except Exception as e:
        logger.error(f"清空目录失败: {str(e)}")

# ==============================================
# 钉钉通知
# ==============================================
def send_dingtalk_message(message, title="自动化任务通知"):
    if not DINGTALK_WEBHOOK or not DINGTALK_SECRET:
        logger.info(f"【通知】{title}\n{message}")
        return False

    try:
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f'{timestamp}\n{DINGTALK_SECRET}'
        hmac_code = hmac.new(DINGTALK_SECRET.encode('utf-8'), string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        webhook_url = f"{DINGTALK_WEBHOOK}&timestamp={timestamp}&sign={sign}"

        data = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": f"## {title}\n\n{message}"},
            "at": {"atMobiles": AT_MOBILES, "isAtAll": AT_ALL}
        }

        response = requests.post(webhook_url, json=data, timeout=10)
        if response.json().get('errcode') == 0:
            logger.info("✅ 钉钉消息发送成功")
            return True
        else:
            logger.error(f"钉钉发送失败: {response.text}")
            return False
    except Exception as e:
        logger.error(f"钉钉消息异常: {str(e)}")
        return False

# ==============================================
# Excel 处理 + 合并 + 去重 + 上传
# ==============================================
def filter_empty_material_rows(df):
    if df.empty or len(df.columns) < 2:
        return df
    return df[~df.iloc[:, 1].isna()]

def process_inventory_data(df):
    if len(df.columns) < 26:
        for i in range(len(df.columns), 26):
            df[f'临时列_{i}'] = ""
    df = df.iloc[:, :26]
    df.columns = INVENTORY_COLUMNS
    return df

def process_purchase_data(df):
    if len(df.columns) < 16:
        for i in range(len(df.columns), 16):
            df[f'临时列_{i}'] = ""
    df = df.iloc[:, :16]
    df.columns = PURCHASE_COLUMNS

    def get_base(p):
        if pd.isna(p): return "上虞基地"
        return "山东基地" if str(p).strip() in ["王群", "任文顺", "孙鑫荣", "范佳呈"] else "上虞基地"
    df["所属基地"] = df["采购负责人"].apply(get_base)
    return df


def delete_specific_inventory_rows(df):
    if df.empty: return df
    cond = (
        ((df.iloc[:,1] == "碳酸二甲酯")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "甲醇")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "丙酮")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "N,N-二甲基甲酰胺")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "无水乙醇")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "氯化亚砜")&(df.iloc[:,4]=="上虞基地")) |
        ((df.iloc[:,1] == "68%哌嗪")&(df.iloc[:,4]=="上虞基地"))
    )
    return df[~cond].reset_index(drop=True)

def check_duplicates(inventory_df, purchase_df):
    dup_list = []
    if inventory_df.empty or len(inventory_df.columns)<2:
        return dup_list
    mat_col = inventory_df.iloc[:,1]
    duplicates = mat_col[mat_col.duplicated(keep=False)]
    if duplicates.empty: return dup_list

    purchaser_map = {}
    if not purchase_df.empty:
        for _, r in purchase_df.iterrows():
            m = str(r.iloc[1]).strip() if len(r)>1 else None
            p = str(r.iloc[3]).strip() if len(r)>3 else None
            if m and p: purchaser_map[m] = p

    for mat in duplicates.unique():
        if pd.isna(mat): continue
        cnt = (mat_col==mat).sum()
        dup_list.append({
            "物料名称":str(mat), "次数":cnt,
            "采购负责人": purchaser_map.get(str(mat).strip(), "未知")
        })
    return dup_list

def remove_duplicates(df, dup_list):
    if not dup_list or df.empty: return df,0
    mats = [d["物料名称"] for d in dup_list]
    res = df[~df.iloc[:,1].isin(mats)]
    for m in mats:
        rows = df[df.iloc[:,1]==m]
        if not rows.empty: res = pd.concat([res, rows.iloc[:1]])
    return res.reset_index(drop=True), len(df)-len(res)

def upload_to_azure(file_path):
    if not os.path.exists(file_path):
        return False, "文件不存在"
    try:
        blob = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container = blob.get_container_client(AZURE_CONTAINER_NAME)
        if not container.exists(): container.create_container()
        with open(file_path,"rb") as f:
            container.upload_blob(OUTPUT_FILENAME, f, overwrite=True)
        return True, f"{AZURE_CONTAINER_NAME}/{OUTPUT_FILENAME}"
    except Exception as e:
        return False, str(e)

def merge_and_upload():
    logger.info("=== 开始Excel合并、去重、上传 ===")
    f1 = os.path.join(DOWNLOAD_FOLDER, FILE1_NAME)
    f2 = os.path.join(DOWNLOAD_FOLDER, FILE2_NAME)
    out = os.path.join(DOWNLOAD_FOLDER, OUTPUT_FILENAME)

    if not os.path.exists(f1) or not os.path.exists(f2):
        msg = f"下载文件缺失\n{f1}\n{f2}"
        logger.error(msg)
        send_dingtalk_message(msg, "合并失败-文件缺失")
        return False

    try:
        inv_sheets, pur_sheets = [], []
        for f, sn in [(f1, pd.ExcelFile(f1).sheet_names), (f2, pd.ExcelFile(f2).sheet_names)]:
            for s in sn:
                if "库存数据" in s: inv_sheets.append((f,s))
                else: pur_sheets.append((f,s))

        inv_list = []
        for f,s in inv_sheets:
            df = pd.read_excel(f,s,header=1)
            df = filter_empty_material_rows(df)
            df = process_inventory_data(df)
            inv_list.append(df)
        final_inv = pd.concat(inv_list,ignore_index=True) if inv_list else pd.DataFrame(columns=INVENTORY_COLUMNS)
        final_inv = delete_specific_inventory_rows(final_inv)

        pur_list = []
        for f,s in pur_sheets:
            df = pd.read_excel(f,s,header=0)
            df = filter_empty_material_rows(df)
            df = process_purchase_data(df)
            pur_list.append(df)
        final_pur = pd.concat(pur_list,ignore_index=True) if pur_list else pd.DataFrame(columns=PURCHASE_COLUMNS)

        with pd.ExcelWriter(out, engine='openpyxl') as w:
            final_inv.to_excel(w, sheet_name="库存数据", index=False)
            final_pur.to_excel(w, sheet_name="采购数据", index=False)

        dup_info = check_duplicates(final_inv, final_pur)
        if dup_info:
            final_inv, del_cnt = remove_duplicates(final_inv, dup_info)
            with pd.ExcelWriter(out, engine='openpyxl') as w:
                final_inv.to_excel(w,"库存数据",index=False)
                final_pur.to_excel(w,"采购数据",index=False)
            dup_msg = f"发现重复物料：{len(dup_info)}个\n已删除重复行：{del_cnt}行"
            logger.warning(dup_msg)
        else:
            dup_msg = "未发现重复物料"
            logger.info(dup_msg)

        up_ok, up_msg = upload_to_azure(out)
        status = "成功" if up_ok else "失败"
        msg = (
            f"合并完成\n库存：{len(final_inv)}行 | 采购：{len(final_pur)}行\n"
            f"{dup_msg}\n上传Azure：{status}\n{up_msg}"
        )
        send_dingtalk_message(msg, f"Excel处理{status}")
        logger.info(msg)
        return up_ok

    except Exception as e:
        err = f"合并异常：{str(e)}"
        logger.error(err)
        send_dingtalk_message(err, "合并异常")
        return False

# ==============================================
# 爬虫主类（核心逻辑：先清空 → 再下载）
# ==============================================
class KdocsAutoCrawler:
    def __init__(self):
        self.urls = URLS
        self.handles = {}
        self.driver = None
        self.wait = None
        self.logged = False
        self.lock = threading.Semaphore(1)
        self.run_flag = True

    def config_driver(self):
        chrome_ops = webdriver.ChromeOptions()
        chrome_ops.add_argument("--disable-blink-features=AutomationControlled")
        chrome_ops.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_ops.add_experimental_option("useAutomationExtension", False)
        chrome_ops.add_experimental_option("detach", True)
        chrome_ops.add_argument(f"user-data-dir={os.path.join(os.getcwd(),'chrome_profile')}")
        prefs = {"profile.managed_default_content_settings.images":1}
        chrome_ops.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_ops)
        self.driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        self.driver.implicitly_wait(10)
        self.wait = WebDriverWait(self.driver,20)
        logger.info("✅ Chrome驱动启动完成")

    def open_tabs(self):
        self.driver.get(self.urls[0])
        self.handles[0] = self.driver.current_window_handle
        for i in range(1,len(self.urls)):
            self.driver.execute_script(f"window.open('{self.urls[i]}','_blank');")
            time.sleep(2)
            self.handles[i] = self.driver.window_handles[-1]
            logger.info(f"✅ 已打开标签页 {i+1}")
        self.driver.switch_to.window(self.handles[0])

    def login(self):
        with self.lock:
            if self.logged: return
            logger.info("🔑 检查登录状态，请手动完成登录")
            time.sleep(15)
            while "login" in self.driver.current_url or "auth" in self.driver.current_url:
                time.sleep(2)
            self.logged = True
            logger.info("✅ 登录成功")

    # ======================
    # 【正确执行顺序】
    # 1. 清空目录
    # 2. 开始下载
    # 3. 合并上传
    # ======================
    def download_all(self):
        logger.info("=== 开始执行自动化任务 ===")
        # 第一步：清空下载目录
        clear_download_folder(DOWNLOAD_FOLDER)
        time.sleep(1)

        # 第二步：开始下载
        logger.info("=== 开始下载报表 ===")
        for i,url in enumerate(self.urls):
            try:
                self.driver.switch_to.window(self.handles[i])
                self.driver.refresh()
                time.sleep(30)
                self.wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="wo-common-header"]/div[1]/span[2]'))).click()
                time.sleep(3)
                self.wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="util-popup"]/div/div/div/div[1]/div[2]'))).click()
                time.sleep(30)
                logger.info(f"✅ 标签页 {i+1} 下载完成")
            except Exception as e:
                logger.error(f"下载失败 {i+1}: {str(e)}")

        # 第三步：合并Excel并上传
        logger.info("=== 下载完成，开始合并 ===")
        merge_and_upload()

    def job(self):
        try:
            if not self.driver:
                self.config_driver()
                self.open_tabs()
                self.login()
            self.download_all()
        except Exception as e:
            logger.error(f"任务异常: {str(e)}")
            send_dingtalk_message(f"爬虫任务异常：{str(e)}","任务失败")

    def schedule_start(self):
        schedule.every().day.at(SCHEDULE_TIME).do(self.job)
        logger.info(f"✅ 定时任务已设置：每日 {SCHEDULE_TIME} 自动执行")
        print(f"程序运行中，{SCHEDULE_TIME}自动执行，Ctrl+C停止")
        while self.run_flag:
            try:
                schedule.run_pending()
                time.sleep(60)
            except KeyboardInterrupt:
                self.run_flag = False
                break

    def run(self):
        try:
            self.config_driver()
            self.open_tabs()
            self.login()
            self.schedule_start()
        finally:
            if self.driver: self.driver.quit()
            logger.info("程序退出")

# ==============================================
# 主入口
# ==============================================
def main():
    try:
        crawler = KdocsAutoCrawler()
        t = threading.Thread(target=crawler.run, daemon=True)
        t.start()
        while True:
            cmd = input("输入 stop 停止：").strip().lower()
            if cmd == "stop":
                crawler.run_flag = False
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("程序被手动终止")

if __name__ == "__main__":
    main()