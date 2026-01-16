# okx_monitor_realtime.py
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

# 全局变量
flag = "0"
price_changes = {}
running = True
clients = set()  # 存储连接的WebSocket客户端
main_event_loop = None  # 存储主事件循环

def format_inst_id(inst_id):
    """格式化产品ID，去掉-USDT-SWAP后缀"""
    if inst_id.endswith('-USDT-SWAP'):
        return inst_id.replace('-USDT-SWAP', '')
    elif inst_id.endswith('-SWAP'):
        return inst_id.replace('-SWAP', '')
    return inst_id

# HTML 模板（修改了CSS和JavaScript）
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>OKX SWAP 涨跌幅实时监控</title>
    <meta charset="utf-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }
        .stat-card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); transition: transform 0.3s; }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-title { font-size: 14px; color: #7f8c8d; text-transform: uppercase; }
        .stat-value { font-size: 28px; font-weight: bold; margin: 10px 0; }
        .positive { color: #27ae60; }
        .negative { color: #e74c3c; }
        .neutral { color: #3498db; }
        .tables-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
        .table-container { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); }
        table { width: 100%; border-collapse: collapse; }
        /* 修改：将所有单元格内容左对齐 */
        th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: bold; position: sticky; top: 0; }
        tr:hover { background: #f5f7fa; }
        .status-bar { background: white; padding: 15px; border-radius: 10px; margin: 20px 0; display: flex; justify-content: space-between; align-items: center; }
        .status-indicator { display: flex; align-items: center; }
        .status-dot { width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .status-connected { background: #27ae60; }
        .status-disconnected { background: #e74c3c; }
        .controls { display: flex; gap: 10px; }
        button { padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; font-weight: bold; transition: all 0.3s; }
        .btn-start { background: #27ae60; color: white; }
        .btn-stop { background: #e74c3c; color: white; }
        .btn-export { background: #3498db; color: white; }
        button:hover { opacity: 0.9; transform: scale(1.05); }
        button:active { transform: scale(0.95); }
        .timestamp { color: #7f8c8d; font-size: 14px; }
        .update-indicator { display: inline-block; width: 10px; height: 10px; background: #27ae60; border-radius: 50%; margin-right: 5px; animation: pulse 2s infinite; }
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        .search-box { 
            margin-bottom: 15px; 
            margin-left: 10px;  /* 添加左边距，使搜索框向左移动 */
            flex-grow: 1;  /* 允许搜索框扩展 */
            max-width: 300px;  /* 限制搜索框最大宽度 */
        }
        .search-box input { 
            width: 100%; 
            padding: 10px; 
            border: 1px solid #ddd; 
            border-radius: 5px; 
            font-size: 14px; 
            box-sizing: border-box;  /* 确保padding不增加宽度 */
        }
        .connection-stats { display: flex; gap: 20px; font-size: 14px; }
        .progress-bar { height: 5px; background: #ecf0f1; border-radius: 3px; margin-top: 10px; overflow: hidden; }
        .progress-fill { height: 100%; background: #27ae60; width: 0%; transition: width 0.5s; }
        @media (max-width: 1200px) {
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            .tables-grid { grid-template-columns: 1fr; }
        }
        @media (max-width: 768px) {
            .stats-grid { grid-template-columns: 1fr; }
        }
        .product-id { 
            font-weight: bold; 
            font-family: 'Consolas', 'Monaco', monospace;
            cursor: pointer;
        }
        .product-id:hover {
            color: #3498db;
            text-decoration: underline;
        }
        /* 数字列样式 */
        .number-cell {
            font-family: 'Consolas', 'Monaco', monospace;
        }
        .positive-number {
            color: #27ae60;
            font-weight: bold;
        }
        .negative-number {
            color: #e74c3c;
            font-weight: bold;
        }
        /* 表格头部样式 */
        .table-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        .table-title {
            margin: 0;
            flex-shrink: 0;  /* 防止标题被压缩 */
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📈 OKX SWAP 涨跌幅实时监控系统</h1>
            <div class="timestamp">
                <span class="update-indicator"></span>
                实时更新中... 最后更新: <span id="last-update">--:--:--</span>
            </div>
        </div>
        
        <div class="status-bar">
            <div class="status-indicator">
                <span class="status-dot status-connected" id="status-dot"></span>
                <span id="status-text">连接正常</span>
            </div>
            <div class="connection-stats">
                <span>产品总数: <span id="total-products">0</span></span>
                <span>数据延迟: <span id="data-latency">0ms</span></span>
                <span>客户端连接: <span id="client-count">1</span></span>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-title">总产品数</div>
                <div class="stat-value neutral" id="total-count">0</div>
                <div class="progress-bar"><div class="progress-fill" id="progress-bar"></div></div>
            </div>
            <div class="stat-card">
                <div class="stat-title">平均涨跌幅</div>
                <div class="stat-value" id="avg-change">0.00%</div>
                <div>上涨/下跌: <span id="up-down-ratio">0/0</span></div>
            </div>
            <div class="stat-card">
                <div class="stat-title">上涨产品</div>
                <div class="stat-value positive" id="up-count">0</div>
                <div>占比: <span id="up-percent">0.0%</span></div>
            </div>
            <div class="stat-card">
                <div class="stat-title">下跌产品</div>
                <div class="stat-value negative" id="down-count">0</div>
                <div>占比: <span id="down-percent">0.0%</span></div>
            </div>
        </div>
        
        <div class="tables-grid">
            <div class="table-container">
                <div class="table-header">
                    <h2 class="table-title">📈 涨幅榜（共<span id="gainers-count">0</span>个）</h2>
                    <div class="search-box">
                        <input type="text" id="search-gainers" placeholder="搜索产品...">
                    </div>
                </div>
                <div style="max-height: 600px; overflow-y: auto;">
                    <table id="gainers-table">
                        <thead>
                            <tr>
                                <th>排名</th>
                                <th>产品</th>
                                <th>涨跌幅</th>
                                <th>开盘价</th>
                                <th>收盘价</th>
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- 实时数据将在这里填充 -->
                        </tbody>
                    </table>
                </div>
            </div>
            
            <div class="table-container">
                <div class="table-header">
                    <h2 class="table-title">📉 跌幅榜（共<span id="losers-count">0</span>个）</h2>
                    <div class="search-box">
                        <input type="text" id="search-losers" placeholder="搜索产品...">
                    </div>
                </div>
                <div style="max-height: 600px; overflow-y: auto;">
                    <table id="losers-table">
                        <thead>
                            <tr>
                                <th>排名</th>
                                <th>产品</th>
                                <th>涨跌幅</th>
                                <th>开盘价</th>
                                <th>收盘价</th>
                                <th>时间</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- 实时数据将在这里填充 -->
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="controls">
            <button class="btn-start" onclick="sendCommand('start')">开始监控</button>
            <button class="btn-stop" onclick="sendCommand('stop')">停止监控</button>
            <button class="btn-export" onclick="sendCommand('export')">导出数据</button>
            <button onclick="sendCommand('clear')" style="background: #f39c12; color: white;">清空数据</button>
            <button onclick="location.reload()" style="background: #95a5a6; color: white;">刷新页面</button>
        </div>
        
        <div class="timestamp" style="text-align: center; margin-top: 20px;">
            系统时间: <span id="system-time">--:--:--</span> | 
            页面加载时间: <span id="page-load-time">--:--:--</span> | 
            数据更新时间: <span id="data-update-time">--:--:--</span>
        </div>
    </div>
    
    <script>
        let ws;
        let pageLoadTime = new Date();
        let lastUpdateTime = new Date();
        let updateCount = 0;
        let currentSearches = { // 存储当前搜索状态
            gainers: '',
            losers: ''
        };
        
        // 初始化WebSocket连接
        function initWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            
            ws = new WebSocket(wsUrl);
            
            ws.onopen = function() {
                console.log('WebSocket连接已建立');
                updateStatus('connected');
                // 发送初始请求获取当前数据
                ws.send(JSON.stringify({type: 'get_data'}));
            };
            
            ws.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    updateCount++;
                    
                    // 根据消息类型处理
                    switch(data.type) {
                        case 'stats_update':
                            updateStats(data.data);
                            break;
                        case 'table_update':
                            updateTables(data.data);
                            break;
                        case 'full_update':
                            updateStats(data.stats);
                            updateTables(data.tables);
                            break;
                        case 'status':
                            showNotification(data.message, data.status);
                            break;
                        case 'command_response':
                            showNotification(data.message, data.success ? 'success' : 'error');
                            break;
                    }
                    
                    // 更新最后更新时间
                    lastUpdateTime = new Date();
                    document.getElementById('last-update').textContent = formatTime(lastUpdateTime);
                    
                    // 计算数据延迟
                    if (data.timestamp) {
                        const latency = new Date() - new Date(data.timestamp);
                        document.getElementById('data-latency').textContent = Math.round(latency) + 'ms';
                    }
                    
                } catch (error) {
                    console.error('处理WebSocket消息时出错:', error);
                }
            };
            
            ws.onclose = function() {
                console.log('WebSocket连接已关闭');
                updateStatus('disconnected');
                // 3秒后尝试重新连接
                setTimeout(initWebSocket, 3000);
            };
            
            ws.onerror = function(error) {
                console.error('WebSocket错误:', error);
                updateStatus('error');
            };
        }
        
        function updateStatus(status) {
            const dot = document.getElementById('status-dot');
            const text = document.getElementById('status-text');
            
            switch(status) {
                case 'connected':
                    dot.className = 'status-dot status-connected';
                    text.textContent = '连接正常';
                    break;
                case 'disconnected':
                    dot.className = 'status-dot status-disconnected';
                    text.textContent = '连接断开';
                    break;
                case 'error':
                    dot.className = 'status-dot status-disconnected';
                    text.textContent = '连接错误';
                    break;
            }
        }
        
        function updateStats(stats) {
            // 更新统计信息
            document.getElementById('total-count').textContent = stats.total;
            // 修改：平均涨跌幅显示小数点后两位
            document.getElementById('avg-change').textContent = stats.avg_change.toFixed(2) + '%';
            document.getElementById('avg-change').className = 'stat-value ' + (stats.avg_change >= 0 ? 'positive' : 'negative');
            document.getElementById('up-count').textContent = stats.up_count;
            document.getElementById('down-count').textContent = stats.down_count;
            document.getElementById('up-percent').textContent = stats.up_percent.toFixed(1) + '%';
            document.getElementById('down-percent').textContent = stats.down_percent.toFixed(1) + '%';
            document.getElementById('up-down-ratio').textContent = stats.up_count + '/' + stats.down_count;
            document.getElementById('total-products').textContent = stats.total;
            
            // 更新进度条
            if (stats.target_total > 0) {
                const progress = (stats.collected / stats.target_total) * 100;
                document.getElementById('progress-bar').style.width = progress + '%';
            }
        }
        
        function updateTables(tables) {
            // 保存当前搜索状态
            saveSearchStates();
            
            // 更新涨幅榜
            updateTable('gainers', tables.gainers);
            
            // 更新跌幅榜
            updateTable('losers', tables.losers);
            
            // 更新数量显示
            document.getElementById('gainers-count').textContent = tables.gainers.length;
            document.getElementById('losers-count').textContent = tables.losers.length;
            
            // 恢复搜索状态
            restoreSearchStates();
        }
        
        function saveSearchStates() {
            // 保存当前搜索框的值
            currentSearches.gainers = document.getElementById('search-gainers').value || '';
            currentSearches.losers = document.getElementById('search-losers').value || '';
        }
        
        function restoreSearchStates() {
            // 恢复搜索状态
            document.getElementById('search-gainers').value = currentSearches.gainers;
            document.getElementById('search-losers').value = currentSearches.losers;
            
            // 应用过滤
            if (currentSearches.gainers) {
                filterTable('gainers', currentSearches.gainers);
            }
            if (currentSearches.losers) {
                filterTable('losers', currentSearches.losers);
            }
        }
        
        function updateTable(tableId, data) {
            const tableBody = document.querySelector(`#${tableId}-table tbody`);
            tableBody.innerHTML = '';
            
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                
                // 存储完整ID用于提示
                const fullId = item.inst_id;
                const displayId = item.display_id || item.inst_id;
                const isPositive = item.change_rate >= 0;
                
                row.innerHTML = `
                    <td>${index + 1}</td>
                    <td>
                        <span class="product-id" title="${fullId}">${displayId}</span>
                    </td>
                    <td class="${isPositive ? 'positive-number' : 'negative-number'} number-cell">
                        ${isPositive ? '+' : ''}${item.change_rate.toFixed(2)}%  <!-- 修改：小数点后两位 -->
                    </td>
                    <td class="number-cell">${formatNumber(item.open_price)}</td>
                    <td class="number-cell">${formatNumber(item.close_price)}</td>
                    <td>${item.timestamp}</td>
                `;
                
                // 添加点击效果
                row.addEventListener('click', () => {
                    row.style.backgroundColor = '#f0f7ff';
                    setTimeout(() => {
                        row.style.backgroundColor = '';
                    }, 500);
                });
                
                tableBody.appendChild(row);
            });
        }
        
        function filterTable(tableId, searchText) {
            const rows = document.querySelectorAll(`#${tableId}-table tbody tr`);
            searchText = searchText.toLowerCase();
            
            let visibleCount = 0;
            
            rows.forEach(row => {
                const cells = row.getElementsByTagName('td');
                let match = false;
                
                for (let cell of cells) {
                    if (cell.textContent.toLowerCase().includes(searchText)) {
                        match = true;
                        break;
                    }
                }
                
                if (match) {
                    row.style.display = '';
                    visibleCount++;
                } else {
                    row.style.display = 'none';
                }
            });
            
            // 更新显示的数量
            document.getElementById(`${tableId}-count`).textContent = visibleCount;
        }
        
        function sendCommand(command) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({type: 'command', command: command}));
            } else {
                showNotification('WebSocket连接未建立', 'error');
            }
        }
        
        function showNotification(message, type) {
            // 创建一个简单的通知
            const notification = document.createElement('div');
            notification.textContent = message;
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 15px 20px;
                background: ${type === 'success' ? '#27ae60' : type === 'error' ? '#e74c3c' : '#3498db'};
                color: white;
                border-radius: 5px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                z-index: 1000;
                animation: slideIn 0.3s ease-out;
            `;
            
            document.body.appendChild(notification);
            
            // 3秒后自动移除
            setTimeout(() => {
                notification.style.animation = 'slideOut 0.3s ease-out';
                setTimeout(() => {
                    document.body.removeChild(notification);
                }, 300);
            }, 3000);
        }
        
        function formatNumber(num) {
            if (num >= 1000) {
                return num.toFixed(2);
            } else if (num >= 1) {
                return num.toFixed(4);
            } else {
                return num.toFixed(8);
            }
        }
        
        function formatTime(date) {
            return date.toLocaleTimeString('zh-CN', { 
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        }
        
        // 更新时间显示
        function updateTimeDisplay() {
            document.getElementById('system-time').textContent = formatTime(new Date());
            document.getElementById('page-load-time').textContent = formatTime(pageLoadTime);
            document.getElementById('data-update-time').textContent = formatTime(lastUpdateTime);
            
            // 更新客户端计数（模拟）
            document.getElementById('client-count').textContent = Math.max(1, Math.floor(updateCount / 10));
        }
        
        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', function() {
            initWebSocket();
            
            // 每秒钟更新时间显示
            setInterval(updateTimeDisplay, 1000);
            
            // 添加CSS动画
            const style = document.createElement('style');
            style.textContent = `
                @keyframes slideIn {
                    from { transform: translateX(100%); opacity: 0; }
                    to { transform: translateX(0); opacity: 1; }
                }
                @keyframes slideOut {
                    from { transform: translateX(0); opacity: 1; }
                    to { transform: translateX(100%); opacity: 0; }
                }
            `;
            document.head.appendChild(style);
            
            // 记录页面加载时间
            pageLoadTime = new Date();
            document.getElementById('page-load-time').textContent = formatTime(pageLoadTime);
            
            // 添加搜索框输入事件监听
            document.getElementById('search-gainers').addEventListener('input', function(e) {
                filterTable('gainers', e.target.value);
            });
            
            document.getElementById('search-losers').addEventListener('input', function(e) {
                filterTable('losers', e.target.value);
            });
        });
        
        // 页面关闭前确认
        window.onbeforeunload = function() {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.close();
            }
        };
    </script>
</body>
</html>
'''

def calculate_change_rate(open_price, close_price):
    """计算涨跌幅百分比"""
    try:
        open_val = float(open_price)
        close_val = float(close_price)
        if open_val == 0:
            return 0
        change_rate = ((close_val - open_val) / open_val) * 100
        return round(change_rate, 4)  # 保持4位精度计算，显示时取2位
    except (ValueError, TypeError):
        return 0

async def okx_websocket_handler():
    """OKX WebSocket处理器"""
    global main_event_loop
    main_event_loop = asyncio.get_event_loop()
    
    marketDataAPI = MarketData.MarketAPI(flag=flag)
    
    # 获取产品列表
    result = marketDataAPI.get_tickers(instType="SWAP")
    
    if result["code"] == "0":
        inst_ids = [item["instId"] for item in result["data"]]
        target_total = len(inst_ids)
        print(f"获取到 {target_total} 个SWAP产品")
    else:
        inst_ids = ["BTC-USDT-SWAP"]
        target_total = 1
        print("获取产品列表失败，使用默认值")
    
    # 连接WebSocket
    ws = WsPublicAsync(url="wss://ws.okx.com:8443/ws/v5/business")
    await ws.start()
    
    # 订阅所有产品
    args = []
    for inst_id in inst_ids:
        args.append({
            "channel": "candle1H",
            "instId": inst_id
        })
    
    print(f"开始订阅 {len(args)} 个产品...")
    
    # 修改：将callback函数改回普通函数（非异步）
    def callback(message):
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # 处理K线数据
            if "data" in data and "arg" in data:
                inst_id = data["arg"]["instId"]
                channel = data["arg"]["channel"]
                
                kline_data = data["data"]
                if kline_data and len(kline_data) > 0:
                    latest_kline = kline_data[0]
                    
                    if len(latest_kline) >= 5:
                        open_price = latest_kline[1]
                        close_price = latest_kline[4]
                        
                        # 计算涨跌幅
                        change_rate = calculate_change_rate(open_price, close_price)
                        
                        # 存储数据
                        price_changes[inst_id] = {
                            'change_rate': change_rate,
                            'open_price': float(open_price),
                            'close_price': float(close_price),
                            'channel': channel,
                            'timestamp': time.time(),
                            'ts': latest_kline[0]
                        }
                        
                        # 修改：使用事件循环安全地调用异步函数
                        # 创建任务但不阻塞
                        if main_event_loop and main_event_loop.is_running():
                            main_event_loop.create_task(broadcast_update())
                        else:
                            # 如果事件循环不在运行，尝试启动它
                            asyncio.run_coroutine_threadsafe(broadcast_update(), main_event_loop)
                        
        except Exception as e:
            print(f"处理消息时出错: {e}")
    
    await ws.subscribe(args, callback=callback)
    
    # 保持连接
    try:
        while running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("收到中断信号...")
    finally:
        # 清理
        await ws.unsubscribe(args, callback=callback)
        await asyncio.sleep(1)
        print("OKX WebSocket连接已关闭")

def get_statistics():
    """获取统计数据"""
    if not price_changes:
        return {
            'total': 0,
            'collected': 0,
            'target_total': 1,
            'avg_change': 0,
            'up_count': 0,
            'down_count': 0,
            'up_percent': 0,
            'down_percent': 0
        }
    
    changes = [data['change_rate'] for data in price_changes.values()]
    total = len(changes)
    
    if total == 0:
        return {
            'total': 0,
            'collected': 0,
            'target_total': 1,
            'avg_change': 0,
            'up_count': 0,
            'down_count': 0,
            'up_percent': 0,
            'down_percent': 0
        }
    
    avg_change = sum(changes) / total
    up_count = len([c for c in changes if c > 0])
    down_count = len([c for c in changes if c < 0])
    
    # 目标总数（从OKX API获取的总数）
    target_total = len(price_changes)  # 这个应该来自初始化时获取的总数
    
    return {
        'total': total,
        'collected': total,
        'target_total': target_total,
        'avg_change': avg_change,
        'up_count': up_count,
        'down_count': down_count,
        'up_percent': (up_count / total) * 100 if total > 0 else 0,
        'down_percent': (down_count / total) * 100 if total > 0 else 0
    }

def get_table_data():
    """获取表格数据 - 修改：显示全部数据，取消前20限制，跌幅榜按跌幅从大到小排序"""
    if not price_changes:
        return {
            'gainers': [],
            'losers': []
        }
    
    # 排序数据
    sorted_data = sorted(
        price_changes.items(),
        key=lambda x: x[1]['change_rate'],
        reverse=True
    )
    
    # 涨幅榜（显示所有上涨产品，取消前20限制，涨幅从大到小排序）
    gainers = []
    for i, (inst_id, data) in enumerate(sorted_data):
        if data['change_rate'] > 0:
            gainers.append({
                'inst_id': inst_id,
                'display_id': format_inst_id(inst_id),  # 添加格式化后的ID
                'change_rate': data['change_rate'],
                'open_price': data['open_price'],
                'close_price': data['close_price'],
                'timestamp': datetime.fromtimestamp(data['timestamp']).strftime("%H:%M:%S")
            })
    
    # 跌幅榜（显示所有下跌产品，取消前20限制，跌幅从大到小排序）
    losers = []
    # 先筛选所有下跌产品
    negative_data = [(inst_id, data) for inst_id, data in sorted_data if data['change_rate'] < 0]
    # 修改：对负数按涨跌幅升序排列（因为负数，升序就是跌幅更大的在前面）
    negative_data.sort(key=lambda x: x[1]['change_rate'])  # 升序排列，负数越小跌幅越大
    
    for i, (inst_id, data) in enumerate(negative_data):
        losers.append({
            'inst_id': inst_id,
            'display_id': format_inst_id(inst_id),  # 添加格式化后的ID
            'change_rate': data['change_rate'],
            'open_price': data['open_price'],
            'close_price': data['close_price'],
            'timestamp': datetime.fromtimestamp(data['timestamp']).strftime("%H:%M:%S")
        })
    
    return {
        'gainers': gainers,
        'losers': losers
    }

async def broadcast_update():
    """广播更新给所有连接的客户端"""
    if not clients:
        return
    
    stats = get_statistics()
    tables = get_table_data()
    
    message = json.dumps({
        'type': 'full_update',
        'timestamp': datetime.now().isoformat(),
        'stats': stats,
        'tables': tables
    })
    
    # 发送给所有客户端
    for ws in list(clients):
        try:
            await ws.send_str(message)
        except:
            # 如果发送失败，从客户端列表中移除
            clients.discard(ws)

async def websocket_handler(request):
    """WebSocket处理器"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # 添加客户端
    clients.add(ws)
    print(f"新客户端连接，当前客户端数: {len(clients)}")
    
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    if data.get('type') == 'get_data':
                        # 发送当前完整数据
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
                        
                        if command == 'start':
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '开始监控命令已发送'
                            }))
                        
                        elif command == 'stop':
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '停止监控命令已发送'
                            }))
                        
                        elif command == 'export':
                            # 导出数据逻辑
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '数据导出功能待实现'
                            }))
                        
                        elif command == 'clear':
                            price_changes.clear()
                            await broadcast_update()
                            await ws.send_str(json.dumps({
                                'type': 'command_response',
                                'success': True,
                                'message': '数据已清空'
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
        # 移除客户端
        clients.discard(ws)
        print(f"客户端断开，当前客户端数: {len(clients)}")
    
    return ws

async def handle_index(request):
    """处理主页请求"""
    return web.Response(text=HTML_TEMPLATE, content_type='text/html')

async def handle_data(request):
    """处理数据API请求"""
    stats = get_statistics()
    tables = get_table_data()
    
    return web.json_response({
        'timestamp': datetime.now().isoformat(),
        'stats': stats,
        'tables': tables
    })

async def handle_command(request):
    """处理命令请求"""
    data = await request.json()
    command = data.get('command')
    
    if command == 'start':
        # 启动监控逻辑
        return web.json_response({'status': 'success', 'message': '监控已启动'})
    
    elif command == 'stop':
        # 停止监控逻辑
        return web.json_response({'status': 'success', 'message': '监控已停止'})
    
    return web.json_response({'status': 'error', 'message': '未知命令'})

async def handle_export(request):
    """处理导出请求"""
    if not price_changes:
        return web.json_response({'status': 'error', 'message': '没有数据可导出'})
    
    try:
        # 创建CSV文件
        filename = f"okx_swap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('产品ID,显示名称,涨跌幅(%),开盘价,收盘价,更新时间\n')
            
            for inst_id, data in price_changes.items():
                timestamp = datetime.fromtimestamp(data['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
                display_name = format_inst_id(inst_id)
                f.write(f'{inst_id},{display_name},{data["change_rate"]:.2f},{data["open_price"]},{data["close_price"]},{timestamp}\n')
        
        return web.json_response({
            'status': 'success', 
            'message': f'数据已导出到: {filename}',
            'filename': filename
        })
    
    except Exception as e:
        return web.json_response({'status': 'error', 'message': f'导出失败: {str(e)}'})

def run_okx_websocket():
    """在新的线程中运行OKX WebSocket"""
    asyncio.run(okx_websocket_handler())

def signal_handler(signum, frame):
    """信号处理函数"""
    global running
    print(f"\n接收到信号 {signum}, 正在停止程序...")
    running = False

async def init_app():
    """初始化应用"""
    app = web.Application()
    
    # 配置CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })
    
    # 添加路由
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/api/data', handle_data)
    app.router.add_post('/api/command', handle_command)
    app.router.add_get('/api/export', handle_export)
    
    # 配置静态文件（如果需要）
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    if not os.path.exists(static_path):
        os.makedirs(static_path)
        print(f"已创建静态文件目录: {static_path}")
    
    app.router.add_static('/static/', static_path, name='static')
    
    # 为所有路由配置CORS
    for route in list(app.router.routes()):
        cors.add(route)
    
    return app

def main():
    """主函数"""
    global running
    
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("OKX SWAP 实时监控系统启动中...")
    
    # 启动OKX WebSocket线程
    ws_thread = threading.Thread(target=run_okx_websocket, daemon=True)
    ws_thread.start()
    
    print("Web服务器启动中...")
    print("访问地址: http://localhost:8080")
    print("按 Ctrl+C 停止程序")
    
    # 启动Web服务器
    web.run_app(init_app(), host='0.0.0.0', port=8080)

if __name__ == "__main__":
    main()