#!/bin/bash
# ============================================================
# 一键初始化脚本 - 宏观流动性仪表盘
# 在终端里运行这个脚本，它会帮你完成大部分工作
# ============================================================

set -e

echo ""
echo "================================================"
echo "  宏观流动性仪表盘 - 一键初始化"
echo "================================================"
echo ""

# 检查 git
if ! command -v git &> /dev/null; then
    echo "❌ 没有找到 git，请先安装："
    echo "   Mac: 打开终端输入 xcode-select --install"
    echo "   Windows: 下载 https://git-scm.com/downloads"
    exit 1
fi
echo "✅ git 已安装"

# 检查 python
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif command -v python &> /dev/null; then
    PYTHON=python
else
    echo "❌ 没有找到 Python，请先安装："
    echo "   下载: https://www.python.org/downloads/"
    exit 1
fi
echo "✅ Python 已安装 ($($PYTHON --version))"

# 检查 pip 依赖
echo ""
echo "📦 安装 Python 依赖..."
$PYTHON -m pip install pandas numpy matplotlib requests pyyaml --quiet
echo "✅ 依赖安装完成"

# 配置 FRED API Key
echo ""
echo "================================================"
echo "  配置 FRED API Key"
echo "================================================"
echo ""
echo "你的 config.yaml 里已经有 FRED Key。"
echo "如果需要更换，请手动编辑 config.yaml 文件。"
echo ""

# 先跑一次 pipeline 验证
echo "================================================"
echo "  测试运行 Pipeline..."
echo "================================================"
echo ""
$PYTHON run_daily.py
echo ""
echo "✅ Pipeline 运行成功！"
echo ""

# 复制 JSON 到 web 目录
mkdir -p web/data
cp output/web/latest.json web/data/latest.json
echo "✅ 数据已复制到 web/data/"

# 初始化 git repo
echo ""
echo "================================================"
echo "  初始化 Git 仓库"
echo "================================================"
echo ""

if [ -d ".git" ]; then
    echo "⚠️  Git 仓库已存在，跳过初始化"
else
    git init
    echo "✅ Git 仓库已初始化"
fi

# 创建 .gitignore
cat > .gitignore << 'GITIGNORE'
# Python
__pycache__/
*.pyc
*.egg-info/

# Cache
cache/

# Output (except web data)
output/
!web/data/

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
GITIGNORE

echo "✅ .gitignore 已创建"

# 初始提交
git add .
git commit -m "init: macro liquidity dashboard" 2>/dev/null || echo "⚠️  没有需要提交的更改"

echo ""
echo "================================================"
echo "  ✅ 本地部分全部完成！"
echo "================================================"
echo ""
echo "接下来你需要手动做 3 件事（见教程）："
echo ""
echo "  1️⃣  创建 GitHub 仓库并推送代码"
echo "  2️⃣  在 GitHub 设置 FRED_API_KEY Secret"
echo "  3️⃣  在 Vercel 导入项目并部署"
echo ""
echo "本地预览: cd web && $PYTHON -m http.server 8080"
echo "然后浏览器打开 http://localhost:8080"
echo ""
