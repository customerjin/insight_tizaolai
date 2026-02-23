#!/bin/bash
# ============================================================
#  宏观流动性仪表盘 - Mac 一键部署
#
#  使用方法：双击这个文件即可运行
#  （首次可能需要右键 → 打开 → 确认运行）
# ============================================================

set -e

# 自动进入脚本所在的目录
cd "$(dirname "$0")"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   宏观流动性仪表盘 - 一键部署               ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "当前目录: $(pwd)"
echo ""

# ---- 第1关：检查工具 ----
echo "🔍 检查必要工具..."
echo ""

# 检查 Python
if command -v python3 &> /dev/null; then
    PYTHON=python3
    echo "  ✅ Python3: $($PYTHON --version 2>&1)"
else
    echo "  ❌ 没找到 Python3"
    echo ""
    echo "  请先安装 Python："
    echo "  打开这个网页下载: https://www.python.org/downloads/"
    echo "  安装完成后重新双击这个文件"
    echo ""
    read -p "按回车键退出..."
    exit 1
fi

# 检查 git
if command -v git &> /dev/null; then
    echo "  ✅ Git: $(git --version 2>&1)"
else
    echo "  ❌ 没找到 Git"
    echo ""
    echo "  请在终端运行: xcode-select --install"
    echo "  安装完成后重新双击这个文件"
    echo ""
    read -p "按回车键退出..."
    exit 1
fi

echo ""
echo "✅ 工具检查通过"
echo ""

# ---- 第2关：安装 Python 依赖 ----
echo "📦 安装 Python 依赖（可能需要1-2分钟）..."
$PYTHON -m pip install pandas numpy matplotlib requests pyyaml --quiet --break-system-packages 2>/dev/null || \
$PYTHON -m pip install pandas numpy matplotlib requests pyyaml --quiet
echo "✅ 依赖安装完成"
echo ""

# ---- 第3关：运行 Pipeline ----
echo "🚀 首次运行数据管线..."
echo "   （从 FRED 和 Yahoo Finance 拉取数据，大约需要1-3分钟）"
echo ""
$PYTHON run_daily.py 2>&1 | while IFS= read -r line; do echo "   $line"; done

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Pipeline 运行失败"
    echo "   最常见原因：网络问题，稍后重试即可"
    read -p "按回车键退出..."
    exit 1
fi

echo ""
echo "✅ Pipeline 运行成功！"
echo ""

# ---- 第4关：准备 Web 文件 ----
echo "📁 准备网页文件..."
mkdir -p web/data
cp output/web/latest.json web/data/latest.json
echo "✅ 数据已复制到 web/data/"
echo ""

# ---- 第5关：初始化 Git ----
echo "📝 初始化 Git 仓库..."

# 创建 .gitignore
cat > .gitignore << 'GITIGNORE'
__pycache__/
*.pyc
*.egg-info/
cache/
output/
!web/data/
.DS_Store
Thumbs.db
.vscode/
.idea/
GITIGNORE

if [ -d ".git" ]; then
    echo "   Git 仓库已存在，跳过初始化"
else
    git init
    echo "   Git 仓库已初始化"
fi

git add .
git commit -m "init: macro liquidity dashboard" 2>/dev/null || echo "   (没有新的更改需要提交)"
echo "✅ Git 准备完毕"
echo ""

# ---- 第6关：推送到 GitHub ----
echo "╔══════════════════════════════════════════════╗"
echo "║   接下来需要你操作几步                      ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "📌 步骤 A：创建 GitHub 仓库"
echo "   1. 打开浏览器，进入: https://github.com/new"
echo "   2. Repository name 填: macro-liquidity"
echo "   3. 选 Private（私有）"
echo "   4. 点 Create repository"
echo "   5. 创建后页面上会显示你的仓库地址"
echo ""

read -p "✏️  请输入你的 GitHub 用户名（例如 xiaojin）: " GITHUB_USER

if [ -z "$GITHUB_USER" ]; then
    echo "❌ 用户名不能为空"
    read -p "按回车键退出..."
    exit 1
fi

REPO_URL="https://github.com/${GITHUB_USER}/macro-liquidity.git"
echo ""
echo "   将使用地址: $REPO_URL"
echo ""

# 检查是否已有 remote
if git remote get-url origin &>/dev/null; then
    git remote set-url origin "$REPO_URL"
    echo "   已更新 remote 地址"
else
    git remote add origin "$REPO_URL"
    echo "   已添加 remote 地址"
fi

echo ""
echo "📤 推送代码到 GitHub..."
echo "   （可能会弹出登录窗口，正常登录即可）"
echo ""

# 尝试推送，main 或 master
git branch -M main 2>/dev/null
if git push -u origin main 2>&1; then
    echo ""
    echo "✅ 代码推送成功！"
else
    echo ""
    echo "⚠️  推送失败，常见原因："
    echo "   1. GitHub 仓库还没创建 → 先去 https://github.com/new 创建"
    echo "   2. 登录信息有误 → 重新尝试"
    echo ""
    echo "   你可以稍后手动运行："
    echo "   git push -u origin main"
    echo ""
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   🎉 本地部分全部完成！                              ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                      ║"
echo "║   还需要做 2 件事（在浏览器里操作）：                ║"
echo "║                                                      ║"
echo "║   1️⃣  设置 FRED API Key（2分钟）                     ║"
echo "║      → 打开 GitHub 仓库                              ║"
echo "║      → Settings → Secrets → Actions                  ║"
echo "║      → New repository secret                         ║"
echo "║      → Name: FRED_API_KEY                            ║"
echo "║      → Value: 你的FRED Key                           ║"
echo "║                                                      ║"
echo "║   2️⃣  部署到 Vercel（3分钟）                         ║"
echo "║      → 打开 https://vercel.com/new                   ║"
echo "║      → 用 GitHub 登录                                ║"
echo "║      → Import 你的 macro-liquidity 仓库              ║"
echo "║      → Root Directory 改成: web                      ║"
echo "║      → 点 Deploy                                     ║"
echo "║                                                      ║"
echo "║   详细图文教程: 打开同目录的「保姆级部署教程.html」  ║"
echo "║                                                      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "📍 本地预览（可选）："
echo "   在终端运行: cd $(pwd)/web && python3 -m http.server 8080"
echo "   然后浏览器打开: http://localhost:8080"
echo ""
read -p "按回车键退出..."
