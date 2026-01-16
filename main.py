import okx.MarketData as MarketData
import asyncio
import json
import time
from datetime import datetime
from okx.websocket.WsPublicAsync import WsPublicAsync

# 假设flag已经定义，如果没有请先定义
flag = "0"  # 0: 实盘，1: 模拟盘

marketDataAPI = MarketData.MarketAPI(flag=flag)

# 1. 获取所有产品行情信息
result = marketDataAPI.get_tickers(instType="SWAP")

# 2. 提取所有instId
if result["code"] == "0":
    inst_ids = [item["instId"] for item in result["data"]]
    print(f"获取到 {len(inst_ids)} 个SWAP产品")
    # 打印前几个看看
    for i, inst_id in enumerate(inst_ids[:5]):
        print(f"  {i+1}. {inst_id}")
else:
    print(f"获取产品列表失败: {result}")
    inst_ids = ["BTC-USDT-SWAP"]  # 默认值

# 创建一个字典来存储所有产品的涨跌幅
price_changes = {}

def calculate_change_rate(open_price, close_price):
    """计算涨跌幅百分比"""
    try:
        open_val = float(open_price)
        close_val = float(close_price)
        if open_val == 0:
            return 0
        change_rate = ((close_val - open_val) / open_val) * 100
        return round(change_rate, 4)  # 保留4位小数
    except (ValueError, TypeError):
        return 0

def callbackFunc(message):
    try:
        # 如果message是字符串，尝试解析为JSON
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message
        
        # 根据消息类型处理
        if "event" in data:
            event = data["event"]
            if event == "subscribe":
                print(f"订阅成功: {data['arg']['instId']} - {data['arg']['channel']}")
            elif event == "error":
                print(f"订阅失败: {data['msg']}")
        elif "data" in data and "arg" in data:
            # 处理K线数据
            inst_id = data["arg"]["instId"]
            channel = data["arg"]["channel"]
            
            # 获取数据
            kline_data = data["data"]
            if kline_data and len(kline_data) > 0:
                # 获取最新的K线数据
                latest_kline = kline_data[0]
                
                # 解析K线数据
                # 格式: ["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"]
                if len(latest_kline) >= 5:
                    open_price = latest_kline[1]
                    close_price = latest_kline[4]
                    confirm = latest_kline[8] if len(latest_kline) > 8 else "0"
                    
                    # 计算涨跌幅
                    change_rate = calculate_change_rate(open_price, close_price)
                    
                    # 存储涨跌幅
                    price_changes[inst_id] = {
                        'change_rate': change_rate,
                        'open_price': open_price,
                        'close_price': close_price,
                        'channel': channel,
                        'timestamp': time.time(),
                        'confirm': confirm,
                        'ts': latest_kline[0]  # 时间戳
                    }
                    
                    # 每收到10个K线更新，就打印一次排序结果
                    if len(price_changes) % 10 == 0:
                        print(f"\n已收到 {len(price_changes)} 个产品的K线数据")
                        
    except json.JSONDecodeError:
        print(f"JSON解析失败，原始消息: {message[:100] if len(str(message)) > 100 else message}")
    except Exception as e:
        print(f"处理消息时出错: {e}")

def get_change_rate_summary():
    """获取涨跌幅统计摘要"""
    if not price_changes:
        return None
    
    changes = [data['change_rate'] for data in price_changes.values()]
    
    # 计算涨幅最大的前5个和跌幅最大的前5个
    sorted_items = sorted(
        price_changes.items(),
        key=lambda x: x[1]['change_rate'],
        reverse=True
    )
    
    top_gainers = sorted_items[:5]
    top_losers = sorted_items[-5:] if len(sorted_items) >= 5 else sorted_items
    
    summary = {
        'total': len(changes),
        'average': sum(changes) / len(changes) if changes else 0,
        'max': max(changes) if changes else 0,
        'min': min(changes) if changes else 0,
        'positive_count': len([c for c in changes if c > 0]),
        'negative_count': len([c for c in changes if c < 0]),
        'zero_count': len([c for c in changes if c == 0]),
        'top_gainers': top_gainers,
        'top_losers': top_losers,
    }
    
    # 计算百分比
    if summary['total'] > 0:
        summary['positive_percent'] = (summary['positive_count'] / summary['total']) * 100
        summary['negative_percent'] = (summary['negative_count'] / summary['total']) * 100
    else:
        summary['positive_percent'] = 0
        summary['negative_percent'] = 0
    
    # 计算标准差
    if changes and len(changes) > 1:
        mean = summary['average']
        variance = sum((x - mean) ** 2 for x in changes) / len(changes)
        summary['std_dev'] = variance ** 0.5
    else:
        summary['std_dev'] = 0
    
    return summary

def display_summary():
    """显示统计摘要"""
    summary = get_change_rate_summary()
    if not summary:
        return
    
    print("\n📊 详细统计摘要:")
    print("-"*80)
    print(f"  总计产品数: {summary['total']}")
    print(f"  平均涨跌幅: {summary['average']:.4f}%")
    print(f"  标准差: {summary['std_dev']:.4f}%")
    print(f"  最高涨幅: {summary['max']:.4f}%")
    print(f"  最大跌幅: {summary['min']:.4f}%")
    print(f"  上涨产品: {summary['positive_count']} ({summary['positive_percent']:.2f}%)")
    print(f"  下跌产品: {summary['negative_count']} ({summary['negative_percent']:.2f}%)")
    print(f"  持平产品: {summary['zero_count']}")
    
    # 显示涨幅前5名
    if summary['top_gainers']:
        print("\n🔥 涨幅前5名:")
        for i, (inst_id, data) in enumerate(summary['top_gainers'], 1):
            print(f"  {i}. {inst_id}: {data['change_rate']:.4f}%")
    
    # 显示跌幅前5名
    if summary['top_losers']:
        print("\n💥 跌幅前5名:")
        for i, (inst_id, data) in enumerate(summary['top_losers'], 1):
            # 确保是负值才显示
            if data['change_rate'] < 0:
                print(f"  {i}. {inst_id}: {data['change_rate']:.4f}%")
    
    print("-"*80)

def sort_and_display_changes():
    """对涨跌幅进行排序并显示结果"""
    if not price_changes:
        print("暂无数据")
        return
    
    # 将字典转换为列表并排序
    sorted_changes = sorted(
        price_changes.items(),
        key=lambda x: x[1]['change_rate'],
        reverse=True  # 按涨跌幅降序排列
    )
    
    # 获取当前时间
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 获取最早和最新数据的时间戳
    if price_changes:
        timestamps = [data['ts'] for data in price_changes.values() if 'ts' in data]
        if timestamps:
            min_ts = min(timestamps)
            max_ts = max(timestamps)
            min_time = datetime.fromtimestamp(int(min_ts)/1000).strftime("%Y-%m-%d %H:%M")
            max_time = datetime.fromtimestamp(int(max_ts)/1000).strftime("%Y-%m-%d %H:%M")
            time_range = f"{min_time} ~ {max_time}"
        else:
            time_range = "未知"
    else:
        time_range = "未知"
    
    print("\n" + "="*100)
    print(f"产品涨跌幅排名 - 更新时间: {current_time}")
    print(f"数据时间范围: {time_range}")
    print(f"总计: {len(sorted_changes)} 个产品")
    print("="*100)
    
    # 显示涨幅前10名
    print("\n📈 涨幅前10名:")
    print("-"*100)
    print(f"{'排名':<5} {'产品ID':<25} {'涨跌幅(%)':<15} {'开盘价':<15} {'收盘价':<15} {'状态':<10}")
    print("-"*100)
    
    for i, (inst_id, data) in enumerate(sorted_changes[:10], 1):
        status = "已完结" if data.get('confirm') == '1' else "进行中"
        print(f"{i:<5} {inst_id:<25} {data['change_rate']:>10.4f}% {data['open_price']:>15} {data['close_price']:>15} {status:>10}")
    
    # 显示跌幅前10名
    print("\n📉 跌幅前10名:")
    print("-"*100)
    print(f"{'排名':<5} {'产品ID':<25} {'涨跌幅(%)':<15} {'开盘价':<15} {'收盘价':<15} {'状态':<10}")
    print("-"*100)
    
    # 注意：跌幅前10名应该是负值最大的前10个
    negative_changes = [(inst_id, data) for inst_id, data in sorted_changes if data['change_rate'] < 0]
    if negative_changes:
        for i, (inst_id, data) in enumerate(negative_changes[:10], 1):
            status = "已完结" if data.get('confirm') == '1' else "进行中"
            print(f"{i:<5} {inst_id:<25} {data['change_rate']:>10.4f}% {data['open_price']:>15} {data['close_price']:>15} {status:>10}")
    else:
        print("暂无下跌产品")
    
    # 显示中位数
    if len(sorted_changes) >= 3:
        mid_index = len(sorted_changes) // 2
        mid_data = sorted_changes[mid_index]
        print(f"\n📊 中位数产品: {mid_data[0]} - 涨跌幅: {mid_data[1]['change_rate']:.4f}%")
    
    # 显示详细统计摘要
    display_summary()
    
    print("\n" + "="*100)

async def periodic_sort_task(interval=60):
    """定期执行排序和显示任务"""
    while True:
        await asyncio.sleep(interval)
        sort_and_display_changes()

async def main():
    ws = WsPublicAsync(url="wss://ws.okx.com:8443/ws/v5/business")
    await ws.start()
    
    # 3. 使用所有instId构建订阅参数，一次性全部订阅
    args = []
    for inst_id in inst_ids:
        args.append({
            "channel": "candle1H",
            "instId": inst_id
        })
    
    print(f"开始订阅 {len(args)} 个产品的K线数据...")
    await ws.subscribe(args, callback=callbackFunc)
    
    # 启动定期排序任务
    sort_task = asyncio.create_task(periodic_sort_task(30))  # 每30秒显示一次
    
    try:
        # 持续运行，直到被中断
        while True:
            await asyncio.sleep(5)
            # 每5秒检查一次，如果有新数据就显示（可选）
            if len(price_changes) > 0 and len(price_changes) % 20 == 0:
                print(f"\n⚡ 实时更新: 已收集 {len(price_changes)} 个产品的K线数据")
                
    except KeyboardInterrupt:
        print("\n正在取消订阅...")
        # 取消定期任务
        sort_task.cancel()
        # 显示最终排序结果
        sort_and_display_changes()
        await ws.unsubscribe(args, callback=callbackFunc)
        await asyncio.sleep(1)
        print("程序结束")
    except Exception as e:
        print(f"WebSocket连接异常: {e}")
        # 显示最终排序结果
        sort_and_display_changes()

if __name__ == "__main__":
    asyncio.run(main())