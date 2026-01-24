# -*- coding: utf-8 -*-
import asyncio
import json
import time
import signal
import os
from datetime import datetime
import okx.MarketData as MarketData
from okx.websocket.WsPublicAsync import WsPublicAsync
from aiohttp import web
import aiohttp_cors
import threading
import copy
import gc
import traceback
from typing import Optional
from collections import deque
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# 全局变量
flag = "0"
price_changes = {}
running = True
clients = set()  # 存储连接的WebSocket客户端
main_event_loop = None  # 存储主事件循环
total_products = 0  # 初始获取的产品总数
inst_ids = []  # 所有产品ID列表
last_received_time = {}  # 记录每个产品最后收到数据的时间
ws_connection_active = False  # WebSocket连接状态标志
volume_24h_data = {}  # 存储24小时成交量数据
volume_last_update = {}  # 记录每个产品24h成交量的最后更新时间

# 内存优化配置
MAX_PRODUCTS = 300  # 限制监控的最大产品数量
MEMORY_CHECK_INTERVAL = 60  # 内存检查间隔（秒）
DATA_CLEANUP_INTERVAL = 300  # 数据清理间隔（秒）

# 重连配置
RECONNECT_DELAY = 5  # 重连延迟（秒）
MAX_RECONNECT_ATTEMPTS = 10  # 最大重连尝试次数
reconnect_attempts = 0  # 当前重连尝试次数

# API请求频率控制 - 优化为0.3秒一次，避免连接被终止
API_RATE_LIMIT_DELAY = 0.3  # API请求间隔（秒），0.3秒=约3.3次/秒
API_BATCH_SIZE = 1  # 每次只更新一个产品，实现连续更新

# 高效数据结构
update_lock = threading.Lock()
broadcast_queue = asyncio.Queue(maxsize=100)  # 限制队列大小

# 添加连续更新队列
volume_update_queue = deque()  # 用于存储待更新成交量的产品ID
volume_update_in_progress = False  # 成交量更新是否正在进行中

class ConnectionManager:
    """连接管理器"""
    
    def __init__(self):
        self.ws = None
        self.connected = False
        self.reconnecting = False
        self.last_heartbeat = time.time()
        self.subscription_args = []
        self.market_api = None  # 不在这里初始化，使用时再创建
        self.last_api_call = 0  # 上次API调用时间
        self.api_request_count = 0  # API请求计数器
        self.api_request_reset_time = time.time()  # 重置计数器的时间
        self.session = None  # 添加session用于API请求
        
    def _get_session(self):
        """创建并配置requests session"""
        if self.session is None:
            self.session = requests.Session()
            # 配置重试策略
            retry_strategy = Retry(
                total=3,  # 最大重试次数
                backoff_factor=1,  # 退避因子
                status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的状态码
                allowed_methods=["GET"]  # 只重试GET请求
            )
            adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
            # 设置超时
            self.session.timeout = 10  # 10秒超时
        return self.session
        
    async def connect(self):
        """建立WebSocket连接"""
        try:
            print("正在连接OKX WebSocket...")
            self.ws = WsPublicAsync(url="wss://ws.okx.com:8443/ws/v5/business")
            await self.ws.start()
            self.connected = True
            self.last_heartbeat = time.time()
            print("OKX WebSocket连接成功")
            return True
        except Exception as e:
            print(f"连接失败: {e}")
            traceback.print_exc()
            return False
    
    async def disconnect(self):
        """断开WebSocket连接"""
        try:
            if self.ws:
                await self.ws.unsubscribe([], callback=lambda x: None)
                self.connected = False
                print("WebSocket连接已断开")
        except Exception as e:
            print(f"断开连接时出错: {e}")
            traceback.print_exc()
        finally:
            self.ws = None
    
    async def subscribe(self, args, callback):
        """订阅数据"""
        try:
            if not self.connected or not self.ws:
                return False
            
            self.subscription_args = args
            await self.ws.subscribe(args, callback=callback)
            print(f"订阅成功，共 {len(args)} 个产品")
            return True
        except Exception as e:
            print(f"订阅失败: {e}")
            traceback.print_exc()
            return False
    
    def is_connected(self):
        """检查连接状态"""
        return self.connected and self.ws is not None
    
    def get_market_api(self):
        """获取MarketAPI实例（延迟创建）"""
        if self.market_api is None:
            self.market_api = MarketData.MarketAPI(flag=flag, debug=False)
        return self.market_api
    
    def get_ticker_data_with_rate_limit(self, inst_id):
        """带速率限制的获取ticker数据"""
        try:
            # 检查频率限制（OKX限制为20次/2秒，即10次/秒）
            current_time = time.time()
            
            # 每2秒重置计数器
            if current_time - self.api_request_reset_time >= 2:
                self.api_request_count = 0
                self.api_request_reset_time = current_time
            
            # 确保不超过频率限制
            if self.api_request_count >= 18:  # 留2次余量
                wait_time = 2.1 - (current_time - self.api_request_reset_time)
                if wait_time > 0:
                    print(f"接近API频率限制，等待{wait_time:.2f}秒... 当前计数: {self.api_request_count}")
                    time.sleep(wait_time)
                    self.api_request_count = 0
                    self.api_request_reset_time = time.time()
            
            # 确保最小请求间隔
            elapsed = current_time - self.last_api_call
            if elapsed < API_RATE_LIMIT_DELAY:
                time.sleep(API_RATE_LIMIT_DELAY - elapsed)
            
            api = self.get_market_api()
            result = api.get_ticker(instId=inst_id)
            self.last_api_call = time.time()
            self.api_request_count += 1
            
            if result and result.get("code") == "0" and result.get("data"):
                return result["data"][0]
            else:
                error_code = result.get("code") if result else "unknown"
                error_msg = result.get("msg") if result else "No response"
                
                if error_code == "50011":  # Too Many Requests
                    print(f"API频率限制触发，等待2秒...")
                    time.sleep(2.1)
                    # 重试一次
                    return self.get_ticker_data_with_rate_limit(inst_id)
                elif error_code == "50113":  # System error
                    print(f"系统错误，等待1秒后重试...")
                    time.sleep(1)
                    return self.get_ticker_data_with_rate_limit(inst_id)
                elif error_code != "0":
                    print(f"获取 {inst_id} ticker数据失败: 代码={error_code}, 消息={error_msg}")
                return None
                
        except requests.exceptions.ConnectionError as e:
            print(f"获取 {inst_id} ticker数据时连接错误: {e}")
            # 连接错误，等待1秒后重试
            time.sleep(1)
            return self.get_ticker_data_with_rate_limit(inst_id)
        except requests.exceptions.Timeout as e:
            print(f"获取 {inst_id} ticker数据时超时: {e}")
            # 超时，等待1秒后重试
            time.sleep(1)
            return self.get_ticker_data_with_rate_limit(inst_id)
        except Exception as e:
            print(f"获取 {inst_id} ticker数据时出错: {e}")
            return None

# 创建连接管理器实例
connection_manager = ConnectionManager()

def format_inst_id(inst_id):
    """格式化产品ID，去掉-USDT-SWAP后缀"""
    if inst_id.endswith('-USDT-SWAP'):
        return inst_id.replace('-USDT-SWAP', '')
    elif inst_id.endswith('-SWAP'):
        return inst_id.replace('-SWAP', '')
    return inst_id

def calculate_24h_volume_usdt(ticker_data):
    """计算24小时成交量（USDT）: ((open24h + last) / 2) * volCcy24h"""
    try:
        open24h = float(ticker_data.get('open24h', 0))
        last = float(ticker_data.get('last', 0))
        volCcy24h = float(ticker_data.get('volCcy24h', 0))
        
        if open24h == 0 or volCcy24h == 0:
            return 0
            
        avg_price = (open24h + last) / 2
        volume_usdt = avg_price * volCcy24h
        return volume_usdt
    except (ValueError, TypeError, KeyError) as e:
        print(f"计算24h成交量出错: {e}")
        return 0

def parse_volume_cn(volume_str):
    """将中文单位成交量字符串解析为数字"""
    if volume_str in ['--', '0', '']:
        return 0
    
    try:
        if '亿' in volume_str:
            return float(volume_str.replace('亿', '')) * 100_000_000
        elif '万' in volume_str:
            return float(volume_str.replace('万', '')) * 10_000
        elif '千' in volume_str:
            return float(volume_str.replace('千', '')) * 1_000
        else:
            return float(volume_str)
    except (ValueError, TypeError):
        return 0

def format_volume_cn(volume):
    """格式化成交量显示（中文单位：万、亿）"""
    if volume == 0:
        return "0"
    
    if volume >= 100_000_000:  # 亿
        return f"{volume/100_000_000:.2f}亿"
    elif volume >= 10_000:  # 万
        return f"{volume/10_000:.2f}万"
    elif volume >= 1000:  # 千
        return f"{volume/1000:.1f}千"
    else:
        return f"{volume:.0f}"

def calculate_change_rate(open_price, close_price):
    """计算涨跌幅百分比"""
    try:
        open_val = float(open_price)
        close_val = float(close_price)
        if open_val == 0:
            return 0
        change_rate = ((close_val - open_val) / open_val) * 100
        return round(change_rate, 2)  # 只保留2位小数
    except (ValueError, TypeError):
        return 0

async def update_single_volume(inst_id, retry_count=0):
    """更新单个产品的24h成交量数据"""
    try:
        ticker_data = connection_manager.get_ticker_data_with_rate_limit(inst_id)
        if ticker_data:
            volume_24h = calculate_24h_volume_usdt(ticker_data)
            volume_24h_data[inst_id] = {
                'volume_24h': volume_24h,
                'volume_24h_formatted': format_volume_cn(volume_24h),
                'last_update': time.time()
            }
            volume_last_update[inst_id] = time.time()
            
            # 立即触发数据更新广播
            if inst_id in price_store.data:
                price_store.update(inst_id, {})
            
            return True
        else:
            # 如果失败，尝试重试（最多3次）
            if retry_count < 3:
                print(f"第{retry_count+1}次重试获取 {inst_id} 成交量数据...")
                await asyncio.sleep(1)  # 等待1秒后重试
                return await update_single_volume(inst_id, retry_count + 1)
            else:
                print(f"获取 {inst_id} 成交量数据失败，已达到最大重试次数")
                return False
    except Exception as e:
        print(f"更新 {inst_id} 24h成交量时出错: {e}")
        if retry_count < 3:
            await asyncio.sleep(1)
            return await update_single_volume(inst_id, retry_count + 1)
        return False

async def continuous_volume_updater():
    """连续更新24h成交量数据 - 每0.3秒更新一个产品"""
    global volume_update_queue, volume_update_in_progress, inst_ids
    
    print("启动连续成交量更新器...")
    
    # 初始化队列
    volume_update_queue = deque(inst_ids)
    
    while running and connection_manager.is_connected():
        try:
            if not volume_update_queue:
                # 重新填充队列
                volume_update_queue = deque(inst_ids)
            
            # 从队列中取出一个产品
            inst_id = volume_update_queue.popleft()
            
            # 更新这个产品的成交量数据
            success = await update_single_volume(inst_id)
            
            if success:
                # 更新完成后，将这个产品放回队列末尾，以便下次更新
                volume_update_queue.append(inst_id)
            else:
                # 如果更新失败，也放回队列末尾，稍后重试
                volume_update_queue.append(inst_id)
                # 等待稍长时间再继续
                await asyncio.sleep(1)
            
            # 等待0.3秒后更新下一个产品
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
            
        except Exception as e:
            print(f"连续成交量更新出错: {e}")
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
    
    print("连续成交量更新器停止")

async def batch_update_volumes():
    """批量更新所有产品的24h成交量数据"""
    global inst_ids
    
    if not inst_ids:
        print("没有产品需要更新成交量数据")
        return
    
    print(f"开始批量更新 {len(inst_ids)} 个产品的24h成交量...")
    
    success_count = 0
    fail_count = 0
    
    # 分批更新，控制API请求频率
    for i in range(0, len(inst_ids), API_BATCH_SIZE):
        batch = inst_ids[i:i+API_BATCH_SIZE]
        
        for inst_id in batch:
            # 如果最近5分钟内已经更新过，跳过
            if inst_id in volume_last_update:
                if time.time() - volume_last_update[inst_id] < 300:  # 5分钟内
                    success_count += 1
                    continue
            
            result = await update_single_volume(inst_id)
            if result:
                success_count += 1
            else:
                fail_count += 1
            
            # 每个请求之间添加延迟，避免频率限制
            await asyncio.sleep(API_RATE_LIMIT_DELAY)
        
        # 更新前端显示
        if main_event_loop and main_event_loop.is_running():
            asyncio.run_coroutine_threadsafe(
                broadcast_volume_stats(),
                main_event_loop
            )
    
    print(f"24h成交量批量更新完成: 成功 {success_count}, 失败 {fail_count}")
    return success_count

async def broadcast_volume_stats():
    """广播成交量更新状态"""
    if not clients:
        return
    
    updated = len([v for v in volume_last_update.values() if time.time() - v < 300])
    total = len(inst_ids)
    
    stats_msg = json.dumps({
        'type': 'volume_update_stats',
        'updated': updated,
        'total': total,
        'timestamp': datetime.now().isoformat()
    })
    
    disconnected_clients = []
    for ws in list(clients):
        try:
            await ws.send_str(stats_msg)
        except:
            disconnected_clients.append(ws)
    
    for ws in disconnected_clients:
        clients.discard(ws)

class MemoryOptimizedDataStore:
    """内存优化的数据存储"""
    
    def __init__(self, max_items=100):
        self.data = {}
        self.max_items = max_items
        self.lock = threading.Lock()
    
    def update(self, key, value):
        """更新数据，如果超过最大限制，删除最旧的数据"""
        with self.lock:
            # 获取24h成交量数据（如果存在）
            volume_24h_info = volume_24h_data.get(key, {})
            
            # 检查数据新鲜度
            volume_freshness = 0  # 0: 无数据, 1: 新鲜(5分钟内), -1: 过期
            if key in volume_last_update:
                last_update = volume_last_update[key]
                if time.time() - last_update < 300:  # 5分钟内
                    volume_freshness = 1
                elif time.time() - last_update < 3600:  # 1小时内
                    volume_freshness = 0
                else:
                    volume_freshness = -1
            
            if len(self.data) >= self.max_items and key not in self.data:
                if self.data:
                    oldest_key = min(self.data.keys(), 
                                   key=lambda k: self.data[k].get('last_update', 0))
                    del self.data[oldest_key]
            
            # 合并现有数据和新的数据
            existing = self.data.get(key, {})
            merged_data = {
                'inst_id': key,
                'change_rate': value.get('change_rate', existing.get('change_rate', 0)),
                'close_price': value.get('close_price', existing.get('close_price', 0)),
                'open_price': value.get('open_price', existing.get('open_price', 0)),
                'volume_1h': value.get('volume_1h', existing.get('volume_1h', 0)),
                'volume_1h_formatted': value.get('volume_1h_formatted', existing.get('volume_1h_formatted', '--')),
                'volume_24h': volume_24h_info.get('volume_24h', existing.get('volume_24h', 0)),
                'volume_24h_formatted': volume_24h_info.get('volume_24h_formatted', existing.get('volume_24h_formatted', '--')),
                'volume_freshness': volume_freshness,
                'timestamp': value.get('timestamp', existing.get('timestamp', time.time())),
                'last_update': time.time()
            }
            
            self.data[key] = merged_data
    
    def get(self, key):
        with self.lock:
            return self.data.get(key)
    
    def get_all(self):
        with self.lock:
            return dict(self.data)
    
    def clear(self):
        with self.lock:
            self.data.clear()
    
    def count(self):
        with self.lock:
            return len(self.data)

price_store = MemoryOptimizedDataStore(max_items=MAX_PRODUCTS)

async def broadcast_connection_status():
    if not clients:
        return
    
    status_msg = json.dumps({
        'type': 'okx_connection_status',
        'status': 'connected' if connection_manager.is_connected() else 'disconnected',
        'timestamp': datetime.now().isoformat(),
        'reconnect_count': reconnect_attempts
    })
    
    disconnected_clients = []
    for ws in list(clients):
        try:
            await ws.send_str(status_msg)
        except:
            disconnected_clients.append(ws)
    
    for ws in disconnected_clients:
        clients.discard(ws)

async def okx_websocket_handler():
    global main_event_loop, total_products, inst_ids, reconnect_attempts, ws_connection_active
    
    print("OKX WebSocket处理器启动...")
    
    main_pairs = [
        "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", 
        "BNB-USDT-SWAP", "XRP-USDT-SWAP", "ADA-USDT-SWAP",
        "DOGE-USDT-SWAP", "DOT-USDT-SWAP", "AVAX-USDT-SWAP",
        "MATIC-USDT-SWAP", "LTC-USDT-SWAP", "LINK-USDT-SWAP",
        "UNI-USDT-SWAP", "ATOM-USDT-SWAP", "FIL-USDT-SWAP",
        "ETC-USDT-SWAP", "XLM-USDT-SWAP", "ALGO-USDT-SWAP"
    ]
    
    def callback(message):
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            if "event" in data and data["event"] == "subscribe":
                return
            
            if "data" in data and "arg" in data:
                inst_id = data["arg"]["instId"]
                kline_data = data["data"]
                
                if kline_data and len(kline_data) > 0:
                    latest_kline = kline_data[0]
                    
                    if len(latest_kline) >= 8:  # 确保有足够的字段
                        open_price = latest_kline[1]
                        close_price = latest_kline[4]
                        volume_1h = float(latest_kline[7]) if latest_kline[7] else 0  # volCcyQuote字段
                        
                        change_rate = calculate_change_rate(open_price, close_price)
                        
                        # 1小时成交量也使用中文单位格式化
                        volume_1h_formatted = format_volume_cn(volume_1h)
                        
                        price_store.update(inst_id, {
                            'change_rate': change_rate,
                            'open_price': float(open_price),
                            'close_price': float(close_price),
                            'volume_1h': volume_1h,
                            'volume_1h_formatted': volume_1h_formatted,
                            'timestamp': time.time()
                        })
                        
                        last_received_time[inst_id] = time.time()
                        
                        try:
                            if main_event_loop and main_event_loop.is_running():
                                if broadcast_queue.qsize() < 50:
                                    asyncio.run_coroutine_threadsafe(
                                        broadcast_queue.put({
                                            'type': 'data_update',
                                            'inst_id': inst_id
                                        }),
                                        main_event_loop
                                    )
                        except:
                            pass
        
        except Exception as e:
            print(f"处理消息时出错: {e}")
    
    async def connect_and_subscribe():
        global reconnect_attempts, inst_ids, total_products, ws_connection_active
        
        try:
            marketDataAPI = MarketData.MarketAPI(flag=flag, debug=False)
            result = marketDataAPI.get_tickers(instType="SWAP")
            
            if result and result.get("code") == "0":
                all_products = [item["instId"] for item in result["data"]]
                inst_ids = []
                for pair in main_pairs:
                    if pair in all_products:
                        inst_ids.append(pair)
                
                remaining_slots = MAX_PRODUCTS - len(inst_ids)
                for product in all_products:
                    if product not in inst_ids and remaining_slots > 0:
                        inst_ids.append(product)
                        remaining_slots -= 1
            else:
                inst_ids = main_pairs[:MAX_PRODUCTS]
        except Exception as e:
            print(f"获取产品列表失败: {e}")
            inst_ids = main_pairs[:min(10, MAX_PRODUCTS)]
        
        total_products = len(inst_ids)
        print(f"选择监控 {total_products} 个产品")
        
        if await connection_manager.connect():
            ws_connection_active = True
            
            # 启动连续成交量更新任务
            if main_event_loop and main_event_loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    continuous_volume_updater(),
                    main_event_loop
                )
            
            batch_size = 10
            for i in range(0, len(inst_ids), batch_size):
                batch = inst_ids[i:i+batch_size]
                args = [{"channel": "candle1H", "instId": inst_id} for inst_id in batch]
                
                if await connection_manager.subscribe(args, callback):
                    await asyncio.sleep(0.5)
                else:
                    print(f"批次 {i//batch_size + 1} 订阅失败")
                    break
            
            print("订阅完成，等待初始数据...")
            await asyncio.sleep(3)
            
            initial_received = price_store.count()
            print(f"初始推送后收到 {initial_received}/{total_products} 个产品数据")
            
            if main_event_loop and main_event_loop.is_running():
                asyncio.run_coroutine_threadsafe(broadcast_connection_status(), main_event_loop)
                asyncio.run_coroutine_threadsafe(broadcast_volume_stats(), main_event_loop)
            
            reconnect_attempts = 0
            return True
        else:
            return False
    
    while running:
        try:
            print("正在建立OKX WebSocket连接...")
            if await connect_and_subscribe():
                print("OKX WebSocket连接成功")
                
                last_data_time = time.time()
                while running and connection_manager.is_connected():
                    await asyncio.sleep(1)
                    
                    current_time = time.time()
                    if current_time - last_data_time > 60:
                        print("长时间没有收到数据，可能连接已断开")
                        break
                    
                    if price_store.count() > 0:
                        last_data_time = current_time
                
                print("OKX WebSocket连接断开")
                ws_connection_active = False
                
                if main_event_loop and main_event_loop.is_running():
                    asyncio.run_coroutine_threadsafe(broadcast_connection_status(), main_event_loop)
            
            await connection_manager.disconnect()
            
            if running:
                reconnect_attempts += 1
                wait_time = min(RECONNECT_DELAY * reconnect_attempts, 60)
                print(f"等待 {wait_time} 秒后重连... (尝试次数: {reconnect_attempts})")
                await asyncio.sleep(wait_time)
                
                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    print(f"达到最大重连尝试次数 {MAX_RECONNECT_ATTEMPTS}")
                    break
        
        except asyncio.CancelledError:
            print("WebSocket任务被取消")
            break
        except Exception as e:
            print(f"WebSocket处理错误: {e}")
            traceback.print_exc()
            if running:
                await asyncio.sleep(RECONNECT_DELAY)
    
    print("OKX WebSocket处理器停止")

def get_statistics():
    try:
        data = price_store.get_all()
        collected = len(data)
        
        if collected == 0:
            return {
                'total': total_products,
                'collected': 0,
                'avg_change': 0,
                'up_count': 0,
                'down_count': 0
            }
        
        changes = [item['change_rate'] for item in data.values()]
        avg_change = sum(changes) / collected
        up_count = len([c for c in changes if c > 0])
        down_count = len([c for c in changes if c < 0])
        
        return {
            'total': total_products,
            'collected': collected,
            'avg_change': avg_change,
            'up_count': up_count,
            'down_count': down_count
        }
    except:
        return {
            'total': 0,
            'collected': 0,
            'avg_change': 0,
            'up_count': 0,
            'down_count': 0
        }

def get_table_data():
    try:
        data = price_store.get_all()
        
        if not data:
            return {'gainers': [], 'losers': []}
        
        gainers = []
        for inst_id, item in data.items():
            if item['change_rate'] > 0:
                gainers.append({
                    'inst_id': inst_id,
                    'display_id': format_inst_id(inst_id),
                    'change_rate': item['change_rate'],
                    'close_price': item['close_price'],
                    'volume_24h': item.get('volume_24h', 0),
                    'volume_24h_formatted': item.get('volume_24h_formatted', '--'),
                    'volume_1h': item.get('volume_1h', 0),
                    'volume_1h_formatted': item.get('volume_1h_formatted', '--'),
                    'volume_freshness': item.get('volume_freshness', 0),
                    'timestamp': datetime.fromtimestamp(item['timestamp']).strftime("%H:%M:%S")
                })
        
        losers = []
        for inst_id, item in data.items():
            if item['change_rate'] < 0:
                losers.append({
                    'inst_id': inst_id,
                    'display_id': format_inst_id(inst_id),
                    'change_rate': item['change_rate'],
                    'close_price': item['close_price'],
                    'volume_24h': item.get('volume_24h', 0),
                    'volume_24h_formatted': item.get('volume_24h_formatted', '--'),
                    'volume_1h': item.get('volume_1h', 0),
                    'volume_1h_formatted': item.get('volume_1h_formatted', '--'),
                    'volume_freshness': item.get('volume_freshness', 0),
                    'timestamp': datetime.fromtimestamp(item['timestamp']).strftime("%H:%M:%S")
                })
        
        gainers.sort(key=lambda x: x['change_rate'], reverse=True)
        losers.sort(key=lambda x: x['change_rate'])
        
        return {
            'gainers': gainers[:50],
            'losers': losers[:50]
        }
    except:
        return {'gainers': [], 'losers': []}

def get_memory_stats():
    import psutil
    import os
    
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        memory_mb = memory_info.rss / 1024 / 1024
        
        return {
            'memory_usage': memory_mb,
            'process_memory': round(memory_mb, 1),
            'collected_data': price_store.count(),
            'subscribed': total_products,
            'clients': len(clients),
            'volume_cache': len(volume_24h_data)
        }
    except:
        return {
            'memory_usage': 0,
            'process_memory': 0,
            'collected_data': price_store.count(),
            'subscribed': total_products,
            'clients': len(clients),
            'volume_cache': len(volume_24h_data)
        }

async def broadcast_worker():
    last_broadcast_time = 0
    broadcast_interval = 1  # 保持1秒更新频率
    last_connection_status_time = 0
    connection_status_interval = 5
    last_volume_stats_time = 0
    volume_stats_interval = 10
    
    while running:
        try:
            current_time = time.time()
            
            if not clients:
                await asyncio.sleep(1)
                continue
            
            if current_time - last_connection_status_time >= connection_status_interval:
                await broadcast_connection_status()
                last_connection_status_time = current_time
            
            if current_time - last_volume_stats_time >= volume_stats_interval:
                await broadcast_volume_stats()
                last_volume_stats_time = current_time
            
            # 使用更高效的数据获取方式
            if current_time - last_broadcast_time >= broadcast_interval:
                stats = get_statistics()
                tables = get_table_data()
                
                broadcast_msg = json.dumps({
                    'type': 'full_update',
                    'timestamp': datetime.now().isoformat(),
                    'stats': stats,
                    'tables': tables
                })
                
                disconnected_clients = []
                for ws in list(clients):
                    try:
                        await ws.send_str(broadcast_msg)
                    except:
                        disconnected_clients.append(ws)
                
                for ws in disconnected_clients:
                    clients.discard(ws)
                
                last_broadcast_time = current_time
            
            # 处理队列中的更新
            try:
                await asyncio.wait_for(broadcast_queue.get(), timeout=0.1)
                broadcast_queue.task_done()
            except asyncio.TimeoutError:
                pass
            
            # 定期清理内存
            if price_store.count() % 50 == 0:
                gc.collect()
            
            await asyncio.sleep(0.05)  # 稍微降低循环频率
            
        except Exception as e:
            print(f"广播工作者出错: {e}")
            await asyncio.sleep(1)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    clients.add(ws)
    client_count = len(clients)
    
    try:
        stats = get_statistics()
        tables = get_table_data()
        
        await ws.send_str(json.dumps({
            'type': 'full_update',
            'timestamp': datetime.now().isoformat(),
            'stats': stats,
            'tables': tables
        }))
        
        await ws.send_str(json.dumps({
            'type': 'okx_connection_status',
            'status': 'connected' if connection_manager.is_connected() else 'disconnected',
            'timestamp': datetime.now().isoformat(),
            'reconnect_count': reconnect_attempts
        }))
        
        await broadcast_volume_stats()
        
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    if data.get('type') == 'get_data':
                        stats = get_statistics()
                        tables = get_table_data()
                        
                        await ws.send_str(json.dumps({
                            'type': 'full_update',
                            'timestamp': datetime.now().isoformat(),
                            'stats': stats,
                            'tables': tables
                        }))
                    
                    elif data.get('type') == 'command':
                        command = data.get('command')
                        
                        if command == 'clear':
                            price_store.clear()
                            last_received_time.clear()
                            volume_24h_data.clear()
                            volume_last_update.clear()
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '数据已清空'
                            }))
                            await broadcast_volume_stats()
                        
                        elif command == 'reconnect':
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '已请求重连'
                            }))
                        
                        elif command == 'update_volumes':
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '开始强制更新所有产品的24h成交量数据...'
                            }))
                            if main_event_loop and main_event_loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    batch_update_volumes(),
                                    main_event_loop
                                )
                    
                    elif data.get('type') == 'get_memory_stats':
                        memory_stats = get_memory_stats()
                        await ws.send_str(json.dumps({
                            'type': 'memory_stats',
                            'timestamp': datetime.now().isoformat(),
                            **memory_stats
                        }))
                    
                    elif data.get('type') == 'get_queue_stats':
                        await ws.send_str(json.dumps({
                            'type': 'queue_stats',
                            'size': broadcast_queue.qsize(),
                            'timestamp': datetime.now().isoformat()
                        }))
                
                except json.JSONDecodeError:
                    pass
            
            elif msg.type == web.WSMsgType.ERROR:
                pass
    
    finally:
        clients.discard(ws)
    
    return ws

# HTML模板（已修复排序和优化性能）
HTML_TEMPLATE = '''<!DOCTYPE html>
<html>
<head>
    <title>OKX SWAP 涨跌幅监控</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        :root {
            --primary: #3498db;
            --success: #27ae60;
            --danger: #e74c3c;
            --warning: #f39c12;
            --gray: #7f8c8d;
            --light: #f8f9fa;
            --dark: #2c3e50;
        }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 15px; background: #f5f5f5; }
        .container { max-width: 1800px; margin: 0 auto; }
        .header { background: var(--dark); color: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 15px 0; }
        .stat-card { background: white; padding: 12px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .stat-value { font-size: 20px; font-weight: bold; margin: 5px 0; }
        .positive { color: var(--success); }
        .negative { color: var(--danger); }
        .tables-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(600px, 1fr)); gap: 15px; margin: 15px 0; }
        .table-container { background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: var(--light); font-weight: 600; }
        .status-bar { background: white; padding: 10px; border-radius: 6px; margin: 15px 0; display: flex; flex-wrap: wrap; gap: 10px; justify-content: space-between; }
        .controls { display: flex; flex-wrap: wrap; gap: 8px; margin: 15px 0; }
        button { padding: 8px 15px; border: none; border-radius: 4px; cursor: pointer; font-weight: 600; font-size: 13px; }
        .btn-start { background: var(--success); color: white; }
        .btn-stop { background: var(--danger); color: white; }
        .search-box { margin: 10px 0; }
        .search-box input { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px; }
        .memory-info { font-size: 12px; color: var(--gray); margin-top: 5px; }
        .memory-warning { color: var(--warning); font-weight: bold; }
        @media (max-width: 768px) {
            .tables-grid { grid-template-columns: 1fr; }
            .table-container { padding: 10px; }
        }
        .compact-table { font-size: 12px; }
        .compact-table th, .compact-table td { padding: 6px 8px; }
        .loading { text-align: center; padding: 20px; color: var(--gray); }
        .update-time { font-size: 12px; color: var(--gray); }
        .product-name { 
            color: var(--primary); 
            font-weight: 500;
        }
        .clickable-row { 
            cursor: pointer; 
        }
        .connection-status {
            font-size: 12px;
            padding: 3px 8px;
            border-radius: 12px;
            background: #e8f4fc;
            color: var(--primary);
        }
        .connection-status.connected {
            background: #e8f6f3;
            color: var(--success);
        }
        .connection-status.disconnected {
            background: #fdeded;
            color: var(--danger);
        }
        .volume-cell {
            text-align: left;
            font-family: 'Microsoft YaHei', 'Courier New', monospace;
        }
        .volume-updated {
            color: #27ae60;
            font-weight: bold;
        }
        .volume-stale {
            color: #e74c3c;
        }
        .sortable-header {
            cursor: pointer;
            user-select: none;
            position: relative;
            padding-right: 20px !important;
        }
        .sortable-header:hover {
            background-color: #e8f4fc;
        }
        .sort-indicator {
            position: absolute;
            right: 5px;
            top: 50%;
            transform: translateY(-50%);
            width: 12px;
            height: 12px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            font-size: 10px;
        }
        .sort-asc .sort-indicator::before {
            content: "▲";
            color: var(--primary);
        }
        .sort-desc .sort-indicator::before {
            content: "▼";
            color: var(--primary);
        }
        .sort-none .sort-indicator::before {
            content: "↕";
            color: #ccc;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h2 style="margin: 0; font-size: 18px;">📈 OKX SWAP 监控 (稳定版)</h2>
                <div class="connection-status" id="okx-connection-status">连接中...</div>
            </div>
            <div class="update-time">
                最后更新: <span id="last-update">--:--:--</span>
                <span id="volume-update-info" style="margin-left: 10px; font-size: 11px;"></span>
            </div>
        </div>
        
        <div class="status-bar">
            <div style="display: flex; align-items: center; gap: 10px;">
                <span id="status-dot" style="width: 10px; height: 10px; border-radius: 50%; background: #27ae60;"></span>
                <span id="status-text">连接正常</span>
            </div>
            <div style="display: flex; gap: 15px; font-size: 13px;">
                <span>产品: <span id="total-count">0</span>/<span id="total-products">0</span></span>
                <span>内存: <span id="memory-usage">-- MB</span></span>
                <span>重连次数: <span id="reconnect-count">0</span></span>
                <span>24h成交量更新: <span id="volume-updated">0</span>/<span id="volume-total">0</span></span>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div style="font-size: 13px; color: var(--gray);">平均涨跌幅</div>
                <div class="stat-value" id="avg-change">0.00%</div>
            </div>
            <div class="stat-card">
                <div style="font-size: 13px; color: var(--gray);">上涨产品</div>
                <div class="stat-value positive" id="up-count">0</div>
            </div>
            <div class="stat-card">
                <div style="font-size: 13px; color: var(--gray);">下跌产品</div>
                <div class="stat-value negative" id="down-count">0</div>
            </div>
        </div>
        
        <div class="tables-grid">
            <div class="table-container">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                    <h3 style="margin: 0; font-size: 15px;">📈 涨幅榜 (<span id="gainers-count">0</span>)</h3>
                    <div style="width: 150px;">
                        <input type="text" id="search-gainers" placeholder="搜索..." style="width: 100%;">
                    </div>
                </div>
                <div style="max-height: 400px; overflow-y: auto;">
                    <table class="compact-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>产品</th>
                                <th>涨跌</th>
                                <th>价格</th>
                                <th class="sortable-header sort-none" data-sort="volume24h" data-table="gainers">24h成交量<div class="sort-indicator"></div></th>
                                <th class="sortable-header sort-none" data-sort="volume1h" data-table="gainers">1h成交量<div class="sort-indicator"></div></th>
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody id="gainers-body">
                            <tr><td colspan="7" class="loading">加载中...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
            
            <div class="table-container">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                    <h3 style="margin: 0; font-size: 15px;">📉 跌幅榜 (<span id="losers-count">0</span>)</h3>
                    <div style="width: 150px;">
                        <input type="text" id="search-losers" placeholder="搜索..." style="width: 100%;">
                    </div>
                </div>
                <div style="max-height: 400px; overflow-y: auto;">
                    <table class="compact-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>产品</th>
                                <th>涨跌</th>
                                <th>价格</th>
                                <th class="sortable-header sort-none" data-sort="volume24h" data-table="losers">24h成交量<div class="sort-indicator"></div></th>
                                <th class="sortable-header sort-none" data-sort="volume1h" data-table="losers">1h成交量<div class="sort-indicator"></div></th>
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody id="losers-body">
                            <tr><td colspan="7" class="loading">加载中...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="controls">
            <button class="btn-start" onclick="sendCommand('start')">开始</button>
            <button class="btn-stop" onclick="sendCommand('stop')">停止</button>
            <button onclick="sendCommand('clear')" style="background: var(--warning); color: white;">清空</button>
            <button onclick="sendCommand('reconnect')" style="background: var(--primary); color: white;">重连</button>
            <button onclick="location.reload()" style="background: var(--gray); color: white;">刷新</button>
            <button onclick="toggleMemoryMonitor()" style="background: var(--primary); color: white;">内存监控</button>
            <button onclick="sendCommand('update_volumes')" style="background: var(--success); color: white;">强制更新成交量</button>
            <button onclick="resetAllSorting()" style="background: var(--warning); color: white;">重置排序</button>
            <div style="flex-grow: 1;"></div>
            <div style="font-size: 12px; color: var(--gray);">
                <span id="queue-size">队列: 0</span> | 
                <span id="client-count">连接: 0</span>
            </div>
        </div>
        
        <div class="memory-info" id="memory-monitor" style="display: none;">
            <div>内存使用详情:</div>
            <div id="memory-details">正在获取...</div>
        </div>
    </div>
    
    <script>
        let ws = null;
        let reconnectTimer = null;
        let memoryMonitorVisible = false;
        let lastUpdateTime = 0;
        let updateQueue = [];
        let isProcessingUpdate = false;
        
        // 性能优化：批量更新
        const BATCH_UPDATE_INTERVAL = 50; // 50ms更新一次，而不是立即更新
        
        // 排序状态 - 修复排序逻辑
        let sortStates = {
            gainers: {
                currentSort: null,  // null: 无排序, volume24h: 按24h成交量, volume1h: 按1h成交量
                sortDirection: 'none', // 'none': 无排序, 'asc': 升序, 'desc': 降序
                data: [],
                originalOrder: []    // 原始涨跌幅排序顺序
            },
            losers: {
                currentSort: null,
                sortDirection: 'none',
                data: [],
                originalOrder: []
            }
        };
        
        // 性能监控
        let performanceStats = {
            totalUpdates: 0,
            lastRenderTime: 0,
            avgRenderTime: 0
        };
        
        function updateOKXConnectionStatus(status) {
            const element = document.getElementById('okx-connection-status');
            element.textContent = status === 'connected' ? 'OKX已连接' : 
                                 status === 'connecting' ? '连接中...' : '连接断开';
            element.className = 'connection-status ' + status;
        }
        
        function initWebSocket() {
            if (ws && ws.readyState === WebSocket.OPEN) return;
            
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            
            ws = new WebSocket(wsUrl);
            
            ws.onopen = () => {
                console.log('WebSocket连接已建立');
                updateStatus('connected');
                ws.send(JSON.stringify({type: 'get_data'}));
                if (reconnectTimer) {
                    clearTimeout(reconnectTimer);
                    reconnectTimer = null;
                }
            };
            
            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    
                    switch(data.type) {
                        case 'full_update':
                            // 使用队列批量处理，避免频繁更新导致的卡顿
                            queueUpdate(() => {
                                updateStats(data.stats);
                                updateTablesWithSort(data.tables);
                                document.getElementById('last-update').textContent = formatTime(new Date());
                            });
                            break;
                        case 'memory_stats':
                            updateMemoryStats(data);
                            break;
                        case 'queue_stats':
                            document.getElementById('queue-size').textContent = `队列: ${data.size}`;
                            break;
                        case 'command_response':
                            showNotification(data.message, data.success ? 'success' : 'error');
                            break;
                        case 'okx_connection_status':
                            updateOKXConnectionStatus(data.status);
                            if (data.reconnect_count !== undefined) {
                                document.getElementById('reconnect-count').textContent = data.reconnect_count;
                            }
                            break;
                        case 'volume_update_stats':
                            updateVolumeStats(data);
                            break;
                    }
                    
                } catch (error) {
                    console.error('处理消息时出错:', error);
                }
            };
            
            ws.onclose = () => {
                console.log('WebSocket连接已关闭');
                updateStatus('disconnected');
                if (!reconnectTimer) {
                    reconnectTimer = setTimeout(initWebSocket, 3000);
                }
            };
            
            ws.onerror = (error) => {
                console.error('WebSocket错误:', error);
                updateStatus('error');
            };
        }
        
        // 批量更新队列
        function queueUpdate(callback) {
            updateQueue.push(callback);
            
            if (!isProcessingUpdate) {
                isProcessingUpdate = true;
                setTimeout(processUpdateQueue, BATCH_UPDATE_INTERVAL);
            }
        }
        
        function processUpdateQueue() {
            if (updateQueue.length === 0) {
                isProcessingUpdate = false;
                return;
            }
            
            const startTime = performance.now();
            
            // 执行所有待处理的更新
            while (updateQueue.length > 0) {
                const callback = updateQueue.shift();
                try {
                    callback();
                } catch (error) {
                    console.error('更新回调出错:', error);
                }
            }
            
            const endTime = performance.now();
            const renderTime = endTime - startTime;
            
            // 更新性能统计
            performanceStats.totalUpdates++;
            performanceStats.lastRenderTime = renderTime;
            performanceStats.avgRenderTime = 
                (performanceStats.avgRenderTime * (performanceStats.totalUpdates - 1) + renderTime) / performanceStats.totalUpdates;
            
            // 如果渲染时间过长，给出警告（仅开发时查看）
            if (renderTime > 100) {
                console.warn(`渲染耗时较长: ${renderTime.toFixed(2)}ms`);
            }
            
            isProcessingUpdate = false;
        }
        
        function updateStatus(status) {
            const dot = document.getElementById('status-dot');
            const text = document.getElementById('status-text');
            
            const colors = {
                connected: '#27ae60',
                disconnected: '#e74c3c',
                error: '#e74c3c'
            };
            
            const texts = {
                connected: '连接正常',
                disconnected: '连接断开',
                error: '连接错误'
            };
            
            dot.style.background = colors[status] || '#e74c3c';
            text.textContent = texts[status] || '未知状态';
        }
        
        function updateStats(stats) {
            document.getElementById('total-count').textContent = stats.collected || 0;
            document.getElementById('total-products').textContent = stats.total || 0;
            
            const avgChangeElement = document.getElementById('avg-change');
            const avgChange = stats.avg_change || 0;
            avgChangeElement.textContent = avgChange.toFixed(2) + '%';
            avgChangeElement.className = 'stat-value ' + (avgChange >= 0 ? 'positive' : 'negative');
            
            document.getElementById('up-count').textContent = stats.up_count || 0;
            document.getElementById('down-count').textContent = stats.down_count || 0;
        }
        
        function updateVolumeStats(data) {
            document.getElementById('volume-updated').textContent = data.updated || 0;
            document.getElementById('volume-total').textContent = data.total || 0;
            
            const infoElement = document.getElementById('volume-update-info');
            if (data.updated >= data.total) {
                infoElement.textContent = '24h成交量: 已更新';
                infoElement.style.color = '#27ae60';
            } else {
                infoElement.textContent = `24h成交量: ${data.updated}/${data.total} 更新中...`;
                infoElement.style.color = '#f39c12';
            }
        }
        
        function parseVolumeValue(volumeStr) {
            // 解析中文单位成交量字符串为数字
            if (volumeStr === '--' || volumeStr === '' || volumeStr === undefined) {
                return -1; // 特殊值，表示没有数据，始终排在末尾
            }
            
            try {
                if (volumeStr.includes('亿')) {
                    return parseFloat(volumeStr.replace('亿', '')) * 100000000;
                } else if (volumeStr.includes('万')) {
                    return parseFloat(volumeStr.replace('万', '')) * 10000;
                } else if (volumeStr.includes('千')) {
                    return parseFloat(volumeStr.replace('千', '')) * 1000;
                } else {
                    return parseFloat(volumeStr);
                }
            } catch (e) {
                return -1;
            }
        }
        
        function sortData(data, sortKey, sortDirection) {
            if (!data || data.length === 0) return data;
            
            // 对数据进行排序，-1表示没有数据，始终排在末尾
            return [...data].sort((a, b) => {
                const aValue = parseVolumeValue(a[sortKey === 'volume24h' ? 'volume_24h_formatted' : 'volume_1h_formatted']);
                const bValue = parseVolumeValue(b[sortKey === 'volume24h' ? 'volume_24h_formatted' : 'volume_1h_formatted']);
                
                // 如果两个都没有数据，保持原顺序
                if (aValue === -1 && bValue === -1) return 0;
                
                // 如果一个有数据，一个没有，有数据的排在前面（无论升序降序）
                if (aValue === -1) return 1; // a没有数据，b有数据，b应该排在前面
                if (bValue === -1) return -1; // b没有数据，a有数据，a应该排在前面
                
                // 两个都有数据，按数值排序
                if (sortDirection === 'asc') {
                    return aValue - bValue; // 从小到大
                } else {
                    return bValue - aValue; // 从大到小
                }
            });
        }
        
        function updateTablesWithSort(tables) {
            const startTime = performance.now();
            
            // 保存原始数据
            if (tables.gainers) {
                const originalData = [...tables.gainers];
                sortStates.gainers.data = originalData;
                
                // 如果是第一次或重置排序，保存原始顺序
                if (!sortStates.gainers.currentSort) {
                    sortStates.gainers.originalOrder = originalData;
                }
            }
            
            if (tables.losers) {
                const originalData = [...tables.losers];
                sortStates.losers.data = originalData;
                
                if (!sortStates.losers.currentSort) {
                    sortStates.losers.originalOrder = originalData;
                }
            }
            
            // 应用当前排序状态
            applyCurrentSort('gainers');
            applyCurrentSort('losers');
            
            document.getElementById('gainers-count').textContent = (tables.gainers || []).length;
            document.getElementById('losers-count').textContent = (tables.losers || []).length;
            
            const endTime = performance.now();
            console.log(`表格更新耗时: ${(endTime - startTime).toFixed(2)}ms`);
        }
        
        function applyCurrentSort(tableType) {
            const state = sortStates[tableType];
            let dataToDisplay;
            
            // 如果没有排序，使用原始涨跌幅排序
            if (!state.currentSort) {
                dataToDisplay = state.originalOrder.length > 0 ? [...state.originalOrder] : [...state.data];
            } else {
                // 应用成交量排序
                dataToDisplay = sortData([...state.data], state.currentSort, state.sortDirection);
            }
            
            // 使用DocumentFragment批量更新DOM
            updateTable(tableType, dataToDisplay);
        }
        
        function updateTable(type, data) {
            const tbody = document.getElementById(`${type}-body`);
            if (!tbody) return;
            
            if (data.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" class="loading">暂无数据</td></tr>';
                return;
            }
            
            // 使用DocumentFragment提高性能
            const fragment = document.createDocumentFragment();
            
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                const isPositive = (item.change_rate || 0) >= 0;
                const volumeFreshness = item.volume_freshness || 0;
                
                // 生成OKX交易链接
                const instId = item.inst_id || '';
                let okxUrl = '';
                if (instId) {
                    const formattedInstId = instId.toLowerCase();
                    okxUrl = `https://www.okx.com/zh-hans/trade-swap/${formattedInstId}`;
                }
                
                // 根据数据新鲜度决定颜色
                let volume24hClass = 'volume-cell';
                let volume24hTitle = '';
                if (volumeFreshness === 1) {
                    volume24hClass += ' volume-updated';
                    volume24hTitle = '24h成交量数据已更新';
                } else if (volumeFreshness === -1) {
                    volume24hClass += ' volume-stale';
                    volume24hTitle = '24h成交量数据已过期';
                }
                
                row.innerHTML = `
                    <td>${index + 1}</td>
                    <td>
                        <span class="product-name">${item.display_id || item.inst_id || ''}</span>
                    </td>
                    <td style="color: ${isPositive ? '#27ae60' : '#e74c3c'}; font-weight: bold;">
                        ${isPositive ? '+' : ''}${(item.change_rate || 0).toFixed(2)}%
                    </td>
                    <td>${formatNumber(item.close_price || 0)}</td>
                    <td class="${volume24hClass}" title="${volume24hTitle}">${item.volume_24h_formatted || '--'}</td>
                    <td class="volume-cell">${item.volume_1h_formatted || '--'}</td>
                    <td>${item.timestamp || '--:--:--'}</td>
                `;
                
                row.className = 'clickable-row';
                
                if (okxUrl) {
                    row.addEventListener('click', function(e) {
                        if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || 
                            e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') {
                            return;
                        }
                        window.open(okxUrl, '_blank');
                    });
                }
                
                fragment.appendChild(row);
            });
            
            // 一次性更新DOM
            tbody.innerHTML = '';
            tbody.appendChild(fragment);
            
            // 更新排序指示器
            updateSortIndicators(type);
        }
        
        function handleSortClick(tableType, sortKey) {
            const state = sortStates[tableType];
            
            // 如果点击的是当前排序的字段，切换方向
            if (state.currentSort === sortKey) {
                // 切换方向：none -> desc -> asc -> none
                if (state.sortDirection === 'none') {
                    state.sortDirection = 'desc';
                } else if (state.sortDirection === 'desc') {
                    state.sortDirection = 'asc';
                } else if (state.sortDirection === 'asc') {
                    // 第三次点击，取消排序，恢复涨跌幅排序
                    state.currentSort = null;
                    state.sortDirection = 'none';
                }
            } else {
                // 点击新字段，设置为降序
                state.currentSort = sortKey;
                state.sortDirection = 'desc';
            }
            
            // 应用排序
            applyCurrentSort(tableType);
        }
        
        function updateSortIndicators(tableType) {
            const state = sortStates[tableType];
            
            // 清除所有排序指示器
            document.querySelectorAll(`[data-table="${tableType}"].sortable-header`).forEach(th => {
                th.classList.remove('sort-none', 'sort-asc', 'sort-desc');
                th.classList.add('sort-none');
            });
            
            // 设置当前排序的指示器
            if (state.currentSort) {
                const th = document.querySelector(`[data-table="${tableType}"][data-sort="${state.currentSort}"]`);
                if (th) {
                    th.classList.remove('sort-none');
                    th.classList.add(`sort-${state.sortDirection}`);
                }
            }
        }
        
        function resetAllSorting() {
            // 重置所有排序状态
            sortStates.gainers.currentSort = null;
            sortStates.gainers.sortDirection = 'none';
            sortStates.losers.currentSort = null;
            sortStates.losers.sortDirection = 'none';
            
            // 应用重置
            applyCurrentSort('gainers');
            applyCurrentSort('losers');
            
            showNotification('排序已重置', 'success');
        }
        
        function updateMemoryStats(data) {
            const usage = data.memory_usage || 0;
            const usageElement = document.getElementById('memory-usage');
            usageElement.textContent = `${usage.toFixed(1)} MB`;
            
            if (usage > 100) {
                usageElement.className = 'memory-warning';
            } else {
                usageElement.className = '';
            }
            
            const details = document.getElementById('memory-details');
            details.innerHTML = `
                进程内存: ${data.process_memory || 0} MB<br>
                已收集数据: ${data.collected_data || 0} 条<br>
                订阅产品: ${data.subscribed || 0} 个<br>
                客户端连接: ${data.clients || 0} 个<br>
                24h成交量缓存: ${data.volume_cache || 0} 个
            `;
        }
        
        function formatNumber(num) {
            if (num >= 1000) return num.toFixed(2);
            if (num >= 1) return num.toFixed(4);
            return num.toFixed(6);
        }
        
        function formatTime(date) {
            return date.toLocaleTimeString('zh-CN', { 
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        }
        
        function sendCommand(command) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({type: 'command', command: command}));
            } else {
                showNotification('连接未建立', 'error');
            }
        }
        
        function showNotification(message, type) {
            console.log(`${type.toUpperCase()}: ${message}`);
            // 使用更轻量的通知
            const notification = document.createElement('div');
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 10px 15px;
                background: ${type === 'success' ? '#27ae60' : '#e74c3c'};
                color: white;
                border-radius: 4px;
                z-index: 1000;
                font-size: 12px;
                animation: fadeInOut 3s ease-in-out;
            `;
            notification.textContent = message;
            document.body.appendChild(notification);
            
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 3000);
        }
        
        // 添加CSS动画
        const style = document.createElement('style');
        style.textContent = `
            @keyframes fadeInOut {
                0% { opacity: 0; transform: translateY(-10px); }
                10% { opacity: 1; transform: translateY(0); }
                90% { opacity: 1; transform: translateY(0); }
                100% { opacity: 0; transform: translateY(-10px); }
            }
        `;
        document.head.appendChild(style);
        
        function toggleMemoryMonitor() {
            const monitor = document.getElementById('memory-monitor');
            memoryMonitorVisible = !memoryMonitorVisible;
            monitor.style.display = memoryMonitorVisible ? 'block' : 'none';
            
            if (memoryMonitorVisible && ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({type: 'get_memory_stats'}));
            }
        }
        
        function initSearch() {
            ['gainers', 'losers'].forEach(type => {
                const input = document.getElementById(`search-${type}`);
                if (input) {
                    input.addEventListener('input', debounce((e) => {
                        const searchText = e.target.value.toLowerCase();
                        const rows = document.querySelectorAll(`#${type}-body tr`);
                        
                        rows.forEach(row => {
                            const cells = row.getElementsByTagName('td');
                            let match = false;
                            
                            for (let cell of cells) {
                                if (cell.textContent.toLowerCase().includes(searchText)) {
                                    match = true;
                                    break;
                                }
                            }
                            
                            row.style.display = match ? '' : 'none';
                        });
                    }, 300));
                }
            });
        }
        
        // 防抖函数
        function debounce(func, wait) {
            let timeout;
            return function executedFunction(...args) {
                const later = () => {
                    clearTimeout(timeout);
                    func(...args);
                };
                clearTimeout(timeout);
                timeout = setTimeout(later, wait);
            };
        }
        
        function initSorting() {
            // 为所有可排序的表头添加点击事件
            document.querySelectorAll('.sortable-header').forEach(th => {
                th.addEventListener('click', function() {
                    const tableType = this.getAttribute('data-table');
                    const sortKey = this.getAttribute('data-sort');
                    
                    if (tableType && sortKey) {
                        handleSortClick(tableType, sortKey);
                    }
                });
            });
        }
        
        document.addEventListener('DOMContentLoaded', () => {
            initWebSocket();
            initSearch();
            initSorting();
            
            // 减少轮询频率，避免卡顿
            setInterval(() => {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    if (memoryMonitorVisible) {
                        ws.send(JSON.stringify({type: 'get_memory_stats'}));
                    }
                }
            }, 3000); // 从1秒改为3秒
        });
        
        window.addEventListener('beforeunload', () => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.close();
            }
        });
    </script>
</body>
</html>'''

async def handle_index(request):
    return web.Response(text=HTML_TEMPLATE, content_type='text/html')

async def handle_data(request):
    stats = get_statistics()
    tables = get_table_data()
    
    return web.json_response({
        'timestamp': datetime.now().isoformat(),
        'stats': stats,
        'tables': tables
    })

async def handle_memory_stats(request):
    memory_stats = get_memory_stats()
    
    return web.json_response({
        'timestamp': datetime.now().isoformat(),
        **memory_stats
    })

async def start_background_tasks(app):
    app['broadcast_worker'] = asyncio.create_task(broadcast_worker())
    
    async def memory_check():
        while running:
            await asyncio.sleep(MEMORY_CHECK_INTERVAL)
            
            memory_stats = get_memory_stats()
            if memory_stats['memory_usage'] > 200:
                print(f"内存使用警告: {memory_stats['memory_usage']:.1f} MB")
                gc.collect()
    
    app['memory_check'] = asyncio.create_task(memory_check())

async def cleanup_background_tasks(app):
    tasks = ['broadcast_worker', 'memory_check']
    for task_name in tasks:
        if task_name in app:
            app[task_name].cancel()
            try:
                await app[task_name]
            except:
                pass

async def init_app():
    global main_event_loop
    
    app = web.Application()
    
    main_event_loop = asyncio.get_event_loop()
    print("主事件循环已保存")
    
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })
    
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/api/data', handle_data)
    app.router.add_get('/api/memory', handle_memory_stats)
    
    for route in list(app.router.routes()):
        cors.add(route)
    
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    
    return app

def run_okx_websocket():
    print("启动OKX WebSocket线程...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(okx_websocket_handler())
    except Exception as e:
        print(f"OKX WebSocket线程错误: {e}")
        traceback.print_exc()
    finally:
        loop.close()

def signal_handler(signum, frame):
    global running
    print(f"\n接收到信号 {signum}, 正在停止程序...")
    running = False

def main():
    global running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("OKX SWAP 实时监控系统启动中...")
    print(f"内存优化配置: 最大产品数={MAX_PRODUCTS}")
    print(f"重连配置: 延迟={RECONNECT_DELAY}秒, 最大尝试={MAX_RECONNECT_ATTEMPTS}")
    print(f"API频率控制: 请求间隔={API_RATE_LIMIT_DELAY}秒 (每0.3秒更新一个产品)")
    print("注意: 24h成交量数据采用连续更新模式，每0.3秒更新一个产品")
    print("优化: 修复了排序逻辑和页面卡顿问题，提升了页面性能")
    print("修复: 增加了API请求的重试机制和连接错误处理")
    
    ws_thread = threading.Thread(target=run_okx_websocket, daemon=True)
    ws_thread.start()
    
    print("Web服务器启动中...")
    print("访问地址: http://localhost:8080")
    print("按 Ctrl+C 停止程序")
    
    try:
        web.run_app(init_app(), host='0.0.0.0', port=8080, access_log=None)
    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"Web服务器错误: {e}")
    finally:
        running = False
        print("程序停止")

if __name__ == "__main__":
    main()