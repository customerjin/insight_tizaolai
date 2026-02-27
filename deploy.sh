#!/bin/bash
# 一键运行 pipeline + 校验 + 推送到 Vercel
# 用法:
#   ./deploy.sh              # 完整 pipeline (宏观 + 每日简报)
#   ./deploy.sh --brief-only # 仅跑每日简报

set -e
cd "$(dirname "$0")"

echo "=================================="
echo "  🚀 开始运行 pipeline..."
echo "=================================="

# 运行 pipeline（自动包含数据自检）
python3 run_daily.py "$@"

# 检查 data/latest.json 是否存在
if [ ! -f data/latest.json ]; then
    echo "❌ data/latest.json 不存在，pipeline 可能失败了"
    exit 1
fi

echo ""
echo "=================================="
echo "  📤 推送到 GitHub + Vercel..."
echo "=================================="

git add data/latest.json
if ! git diff --staged --quiet; then
    git commit -m "chore: update data $(date +%Y-%m-%d_%H:%M)"
    git push
    echo "✅ 推送成功！Vercel 将在 ~1 分钟内自动部署"
    echo "🌐 https://invest-wine.vercel.app/"
    echo "💡 如果页面没更新，用 Cmd+Shift+R 强制刷新，或加 ?v=$(date +%s) 破缓存"
else
    echo "ℹ️  数据无变化，无需推送"
fi
