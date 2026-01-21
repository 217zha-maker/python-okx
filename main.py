# okx_monitor_realtime_fixed_no_latency.py
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

# 内存优化配置
MAX_PRODUCTS = 300  # 限制监控的最大产品数量
MEMORY_CHECK_INTERVAL = 60  # 内存检查间隔（秒）
DATA_CLEANUP_INTERVAL = 300  # 数据清理间隔（秒）

# 重连配置
RECONNECT_DELAY = 5  # 重连延迟（秒）
MAX_RECONNECT_ATTEMPTS = 10  # 最大重连尝试次数
reconnect_attempts = 0  # 当前重连尝试次数

# 高效数据结构
update_lock = threading.Lock()
broadcast_queue = asyncio.Queue(maxsize=100)  # 限制队列大小

class ConnectionManager:
    """连接管理器"""
    
    def __init__(self):
        self.ws = None
        self.connected = False
        self.reconnecting = False
        self.last_heartbeat = time.time()
        self.subscription_args = []
        
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
                # 注意：原okx库可能没有提供close方法，这里尝试安全断开
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

# 创建连接管理器实例
connection_manager = ConnectionManager()

def format_inst_id(inst_id):
    """格式化产品ID，去掉-USDT-SWAP后缀"""
    if inst_id.endswith('-USDT-SWAP'):
        return inst_id.replace('-USDT-SWAP', '')
    elif inst_id.endswith('-SWAP'):
        return inst_id.replace('-SWAP', '')
    return inst_id

# HTML模板（已移除 data-latency 相关部分）
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
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: var(--dark); color: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 15px 0; }
        .stat-card { background: white; padding: 12px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .stat-value { font-size: 20px; font-weight: bold; margin: 5px 0; }
        .positive { color: var(--success); }
        .negative { color: var(--danger); }
        .tables-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 15px; margin: 15px 0; }
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h2 style="margin: 0; font-size: 18px;">📈 OKX SWAP 监控 (修复版)</h2>
                <div class="connection-status" id="okx-connection-status">连接中...</div>
            </div>
            <div class="update-time">
                最后更新: <span id="last-update">--:--:--</span>
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
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody id="gainers-body">
                            <tr><td colspan="5" class="loading">加载中...</td></tr>
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
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody id="losers-body">
                            <tr><td colspan="5" class="loading">加载中...</td></tr>
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
        let updateCount = 0;
        let memoryMonitorVisible = false;
        
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
                    updateCount++;
                    
                    switch(data.type) {
                        case 'full_update':
                            updateStats(data.stats);
                            updateTables(data.tables);
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
                    }
                    
                    document.getElementById('last-update').textContent = formatTime(new Date());
                    
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
        
        function updateTables(tables) {
            updateTable('gainers', tables.gainers || []);
            updateTable('losers', tables.losers || []);
            
            document.getElementById('gainers-count').textContent = (tables.gainers || []).length;
            document.getElementById('losers-count').textContent = (tables.losers || []).length;
        }
        
        function updateTable(type, data) {
            const tbody = document.getElementById(`${type}-body`);
            if (!tbody) return;
            
            tbody.innerHTML = '';
            
            if (data.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="loading">暂无数据</td></tr>';
                return;
            }
            
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                const isPositive = (item.change_rate || 0) >= 0;
                
                // 生成OKX交易链接
                const instId = item.inst_id || '';
                let okxUrl = '';
                if (instId) {
                    const formattedInstId = instId.toLowerCase();
                    okxUrl = `https://www.okx.com/zh-hans/trade-swap/${formattedInstId}`;
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
                
                tbody.appendChild(row);
            });
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
                客户端连接: ${data.clients || 0} 个
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
        }
        
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
                    input.addEventListener('input', (e) => {
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
                    });
                }
            });
        }
        
        document.addEventListener('DOMContentLoaded', () => {
            initWebSocket();
            initSearch();
            
            setInterval(() => {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    if (memoryMonitorVisible) {
                        ws.send(JSON.stringify({type: 'get_memory_stats'}));
                    }
                    ws.send(JSON.stringify({type: 'get_queue_stats'}));
                }
            }, 1000);
        });
        
        window.addEventListener('beforeunload', () => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.close();
            }
        });
    </script>
</body>
</html>'''

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

class MemoryOptimizedDataStore:
    """内存优化的数据存储"""
    
    def __init__(self, max_items=100):
        self.data = {}
        self.max_items = max_items
        self.lock = threading.Lock()
    
    def update(self, key, value):
        """更新数据，如果超过最大限制，删除最旧的数据"""
        with self.lock:
            if len(self.data) >= self.max_items and key not in self.data:
                if self.data:
                    oldest_key = min(self.data.keys(), 
                                   key=lambda k: self.data[k].get('last_update', 0))
                    del self.data[oldest_key]
            
            self.data[key] = {
                'change_rate': value.get('change_rate', 0),
                'close_price': value.get('close_price', 0),
                'open_price': value.get('open_price', 0),
                'timestamp': time.time(),
                'last_update': time.time()
            }
    
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
                print(f"订阅成功: {data['arg']}")
                return
            
            if "data" in data and "arg" in data:
                inst_id = data["arg"]["instId"]
                kline_data = data["data"]
                
                if kline_data and len(kline_data) > 0:
                    latest_kline = kline_data[0]
                    
                    if len(latest_kline) >= 5:
                        open_price = latest_kline[1]
                        close_price = latest_kline[4]
                        
                        change_rate = calculate_change_rate(open_price, close_price)
                        
                        price_store.update(inst_id, {
                            'change_rate': change_rate,
                            'open_price': float(open_price),
                            'close_price': float(close_price),
                            'timestamp': time.time()
                        })
                        
                        last_received_time[inst_id] = time.time()
                        
                        collected = price_store.count()
                        if collected > 0 and collected % 10 == 0:
                            print(f"已收集 {collected}/{total_products} 个产品数据")
                        
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
            traceback.print_exc()
    
    async def connect_and_subscribe():
        global reconnect_attempts, inst_ids, total_products, ws_connection_active
        
        try:
            marketDataAPI = MarketData.MarketAPI(flag=flag)
            result = marketDataAPI.get_tickers(instType="SWAP")
            
            if result["code"] == "0":
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
            
            batch_size = 10
            for i in range(0, len(inst_ids), batch_size):
                batch = inst_ids[i:i+batch_size]
                args = [{"channel": "candle1H", "instId": inst_id} for inst_id in batch]
                
                print(f"订阅批次 {i//batch_size + 1}，数量: {len(batch)}")
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
            'clients': len(clients)
        }
    except:
        return {
            'memory_usage': 0,
            'process_memory': 0,
            'collected_data': price_store.count(),
            'subscribed': total_products,
            'clients': len(clients)
        }

async def broadcast_worker():
    last_broadcast_time = 0
    broadcast_interval = 1
    last_connection_status_time = 0
    connection_status_interval = 5
    
    while running:
        try:
            current_time = time.time()
            
            if not clients:
                await asyncio.sleep(1)
                continue
            
            if current_time - last_connection_status_time >= connection_status_interval:
                await broadcast_connection_status()
                last_connection_status_time = current_time
            
            if current_time - last_broadcast_time < broadcast_interval:
                try:
                    await asyncio.wait_for(broadcast_queue.get(), timeout=0.5)
                    broadcast_queue.task_done()
                except asyncio.TimeoutError:
                    pass
                
                await asyncio.sleep(0.1)
                continue
            
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
            
            if price_store.count() % 20 == 0:
                gc.collect()
            
            await asyncio.sleep(0.1)
            
        except Exception as e:
            print(f"广播工作者出错: {e}")
            await asyncio.sleep(1)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    clients.add(ws)
    client_count = len(clients)
    print(f"新客户端连接，当前客户端数: {client_count}")
    
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
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '数据已清空'
                            }))
                        
                        elif command == 'reconnect':
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '已请求重连'
                            }))
                            print("收到重连命令")
                    
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
                    await ws.send_str(json.dumps({
                        'type': 'status',
                        'status': 'error',
                        'message': '无效的JSON数据'
                    }))
            
            elif msg.type == web.WSMsgType.ERROR:
                print(f'WebSocket错误: {ws.exception()}')
    
    finally:
        clients.discard(ws)
        print(f"客户端断开，当前客户端数: {len(clients)}")
    
    return ws

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