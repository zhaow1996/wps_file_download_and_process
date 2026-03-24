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

# 定义要打开的多个网址
urls = [
    'https://www.kdocs.cn/l/cibolyZ87S0l',  # 原料一科月报表
    'https://www.kdocs.cn/l/cmIhGPuoC7mD'  # 原料二科月报表
]

driver_path = r"C:\Users\Administrator\Desktop\report\chromedriver.exe"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kdocs_crawler.log'),
        logging.StreamHandler()
    ]
)


class MultiTabCrawler():
    def __init__(self, urls):
        self.urls = urls
        self.tab_handles = {}  # 存储标签页句柄
        self.driver = None
        self.wait = None
        self.is_logged_in = False
        self.login_semaphore = threading.Semaphore(1)  # 确保登录只执行一次
        self.should_run = True
        logging.info(f"爬虫初始化完成，共{len(urls)}个标签页")

    def configure_driver(self):
        '''配置Chrome WebDriver并打开多个标签页'''
        try:
            chrome_options = webdriver.ChromeOptions()

            # 禁用自动化控制标志
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # 保持浏览器打开
            chrome_options.add_experimental_option("detach", True)

            # 可选：设置无头模式（后台运行）
            # chrome_options.add_argument("--headless")

            # 启用图片加载
            prefs = {"profile.managed_default_content_settings.images": 1}
            chrome_options.add_experimental_option("prefs", prefs)

            # 添加用户数据目录，保持登录状态
            chrome_options.add_argument(f"user-data-dir={os.path.join(os.getcwd(), 'chrome_profile')}")

            # 创建Service对象并初始化WebDriver
            service = Service(executable_path=driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # 设置等待
            self.driver.implicitly_wait(10)
            self.wait = WebDriverWait(self.driver, 15)

            logging.info("WebDriver配置完成")

            # 打开多个标签页
            self.open_multiple_tabs()

        except Exception as e:
            logging.error(f"配置WebDriver失败: {str(e)}")
            raise

    def open_multiple_tabs(self):
        '''打开多个标签页'''
        try:
            # 打开第一个标签页
            logging.info(f"打开第一个标签页: {self.urls[0]}")
            self.driver.get(self.urls[0])
            self.tab_handles[0] = self.driver.current_window_handle

            # 打开其他标签页
            for i in range(1, len(self.urls)):
                # 通过JavaScript打开新标签页
                self.driver.execute_script(f"window.open('{self.urls[i]}', '_blank');")
                time.sleep(2)  # 等待新标签页打开

                # 获取所有标签页句柄，找到新打开的标签页
                handles = self.driver.window_handles
                new_handle = handles[-1]  # 最新打开的标签页
                self.tab_handles[i] = new_handle

                logging.info(f"打开第{i + 1}个标签页: {self.urls[i]}")
                time.sleep(2)

            # 切换回第一个标签页
            self.driver.switch_to.window(self.tab_handles[0])
            logging.info(f"已打开{len(self.tab_handles)}个标签页")

        except Exception as e:
            logging.error(f"打开多个标签页失败: {str(e)}")
            raise

    def switch_to_tab(self, tab_index):
        '''切换到指定标签页'''
        try:
            if tab_index in self.tab_handles:
                self.driver.switch_to.window(self.tab_handles[tab_index])
                logging.info(f"切换到标签页 {tab_index + 1}")
                time.sleep(2)  # 等待页面加载
                return True
            else:
                logging.error(f"标签页 {tab_index} 不存在")
                return False
        except Exception as e:
            logging.error(f"切换标签页失败: {str(e)}")
            return False

    def login(self):
        '''登录网站（只执行一次）'''
        with self.login_semaphore:
            if not self.is_logged_in:
                try:
                    logging.info("正在执行登录...")
                    print("正在检查登录状态...")

                    # 切换到第一个标签页检查登录状态
                    self.switch_to_tab(0)

                    # 检查是否已登录
                    time.sleep(20)
                    if "login" in self.driver.current_url or "auth" in self.driver.current_url:
                        logging.info("检测到需要登录，请手动登录...")
                        print("请在浏览器中完成登录，登录成功后程序会自动继续...")

                        # 等待用户手动登录
                        while True:
                            time.sleep(2)
                            if "login" not in self.driver.current_url and "auth" not in self.driver.current_url:
                                logging.info("登录成功检测")
                                self.is_logged_in = True
                                break
                    else:
                        logging.info("已检测到登录状态")
                        self.is_logged_in = True

                except Exception as e:
                    logging.error(f"登录过程中发生错误: {str(e)}")
                    raise
            else:
                logging.info("已登录，跳过登录步骤")

    def download_from_tab(self, tab_index, url):
        '''从指定标签页下载文件'''
        try:
            logging.info(f"开始从标签页 {tab_index + 1} 下载，URL: {url}")

            # 切换到指定标签页
            if not self.switch_to_tab(tab_index):
                return False

            # 刷新页面确保是最新状态
            logging.info(f"刷新标签页 {tab_index + 1}...")
            self.driver.refresh()
            time.sleep(30)  # 等待页面加载

            try:
                logging.info(f"点击标签页 {tab_index + 1} 的文件操作按钮...")
                home_button = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="wo-common-header"]/div[1]/span[2]'))
                )
                home_button.click()
                time.sleep(5)

                logging.info(f"点击标签页 {tab_index + 1} 的下载按钮...")
                download_button = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="util-popup"]/div/div/div/div[1]/div[2]'))
                )
                download_button.click()

                logging.info(f"标签页 {tab_index + 1} 下载任务已启动，等待下载完成...")

                # 等待下载完成
                time.sleep(30)

                logging.info(f"标签页 {tab_index + 1} 下载完成")
                return True

            except Exception as e:
                logging.error(f"标签页 {tab_index + 1} 下载过程中发生错误: {str(e)}")
                # 尝试重新加载页面
                try:
                    logging.info(f"尝试重新加载标签页 {tab_index + 1}...")
                    self.driver.refresh()
                    time.sleep(20)
                except:
                    pass
                return False

        except Exception as e:
            logging.error(f"处理标签页 {tab_index + 1} 时发生错误: {str(e)}")
            return False

    def download_all(self):
        '''下载所有标签页的文件'''
        try:
            logging.info(f"开始下载所有标签页的任务，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # 如果浏览器被关闭，重新打开
            if not self.driver or not hasattr(self.driver, 'window_handles'):
                logging.warning("浏览器被关闭，正在重新启动...")
                self.configure_driver()
                # 重新登录
                self.is_logged_in = False
                self.login()

            # 依次下载每个标签页
            for i, url in enumerate(self.urls):
                try:
                    self.download_from_tab(i, url)

                    # 每个标签页下载完成后等待一段时间
                    if i < len(self.urls) - 1:
                        logging.info(f"等待5秒后处理下一个标签页...")
                        time.sleep(5)

                except Exception as e:
                    logging.error(f"处理标签页 {i + 1} 时发生错误: {str(e)}")
                    continue

            logging.info(f"所有标签页下载任务完成，时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logging.error(f"下载所有标签页时发生错误: {str(e)}")
            # 尝试截图保存错误信息
            try:
                screenshot_path = f"error_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.driver.save_screenshot(screenshot_path)
                logging.info(f"错误截图已保存到: {screenshot_path}")
            except:
                pass
            raise

    def schedule_download(self):
        '''定时执行下载任务'''
        logging.info(f"设置定时任务: 每天08:45执行下载所有标签页")

        # 设置定时任务
        schedule.every().day.at("08:45").do(self.download_wrapper)

        # 如果需要立即测试，可以取消下面的注释
        # schedule.every(1).minutes.do(self.download_wrapper)  # 每分钟执行一次，用于测试

        logging.info("定时任务已启动，程序将持续运行...")
        print(f"程序已启动，将在每天08:45自动执行下载所有{len(self.urls)}个标签页")
        print("按 Ctrl+C 可停止程序")

        while self.should_run:
            try:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            except KeyboardInterrupt:
                logging.info("接收到中断信号，正在停止程序...")
                self.should_run = False
                break
            except Exception as e:
                logging.error(f"定时任务执行出错: {str(e)}")
                time.sleep(60)

    def download_wrapper(self):
        '''下载任务包装器，用于错误处理'''
        try:
            self.download_all()
        except Exception as e:
            logging.error(f"定时下载任务失败: {str(e)}")

    def run(self):
        '''执行主流程'''
        try:
            logging.info("启动多标签页爬虫...")

            # 配置和登录
            self.configure_driver()
            self.login()

            # 立即执行一次下载（可选）
            # logging.info("立即执行首次下载...")
            # self.download_all()

            # 启动定时任务
            self.schedule_download()

        except Exception as e:
            logging.error(f"爬虫执行失败: {str(e)}")
        finally:
            self.cleanup()

    def cleanup(self):
        '''清理资源'''
        logging.info("正在清理资源...")
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        logging.info("程序结束")


def main():
    '''主函数'''
    crawler = MultiTabCrawler(urls)

    # 创建并启动守护线程
    crawler_thread = threading.Thread(target=crawler.run)
    crawler_thread.daemon = True
    crawler_thread.start()

    # 主线程等待用户输入
    try:
        while True:
            cmd = input("输入 'stop' 停止程序: ").strip().lower()
            if cmd == 'stop':
                logging.info("接收到停止命令")
                crawler.should_run = False
                crawler.cleanup()
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
        crawler.should_run = False
        crawler.cleanup()


if __name__ == '__main__':
    main()