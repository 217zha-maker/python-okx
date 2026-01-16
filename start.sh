echo "启动OKX SWAP监控系统..."

# 1. 创建必要的目录
mkdir -p visualization

# 2. 启动主程序（后台运行）
python3 main.py &
PID=$!

echo "主程序已启动，PID: $PID"
echo "等待数据收集..."

# 3. 等待60秒让数据积累
sleep 60

# 4. 启动HTTP服务器
echo "启动HTTP服务器..."
python3 -m http.server 8000 --directory visualization &
SERVER_PID=$!

echo "HTTP服务器已启动，PID: $SERVER_PID"
echo "打开浏览器访问: http://localhost:8000/"
echo ""
echo "按 Ctrl+C 停止所有服务"

# 5. 等待用户中断
trap "kill $PID $SERVER_PID 2>/dev/null; exit" INT TERM
wait

echo "服务已停止"

# 下面指令开始运行 
# chmod +x start.sh
# ./start.sh