# 部署指南：宏观流动性仪表盘

## 架构

```
GitHub Actions (每日08:00 UTC+8)
    │
    ├── 拉代码
    ├── pip install pandas numpy matplotlib requests pyyaml
    ├── python run_daily.py  ← 调FRED/Yahoo API拉数据 → 计算 → 生成JSON
    ├── cp output/web/latest.json → web/data/latest.json
    └── git commit & push
            │
            ▼
    Vercel 检测到 push → 自动部署 web/ 目录
            │
            ▼
    用户访问 → index.html → fetch("./data/latest.json") → 渲染
```

## 第一步：创建 GitHub Repo

```bash
# 在本地 macro_liquidity_daily 目录
cd macro_liquidity_daily
git init
git add .
git commit -m "init: macro liquidity dashboard"

# 在 GitHub 创建 repo (假设叫 macro-liquidity)
gh repo create macro-liquidity --private --push
# 或手动创建后:
git remote add origin git@github.com:YOUR_USER/macro-liquidity.git
git push -u origin main
```

## 第二步：配置 GitHub Secrets

进入 GitHub repo → Settings → Secrets and variables → Actions → New repository secret:

- `FRED_API_KEY` = `你的FRED API Key`

（这样 key 不会暴露在代码里）

## 第三步：部署到 Vercel

1. 登录 [vercel.com](https://vercel.com)
2. "Add New Project" → Import your GitHub repo
3. 配置：
   - **Framework Preset**: Other
   - **Root Directory**: `web`  ← 重要！指向 web/ 子目录
   - **Build Command**: (留空)
   - **Output Directory**: `.`
4. Deploy

Vercel 会给你一个 URL，比如 `macro-liquidity.vercel.app`

## 第四步：手动触发第一次更新

GitHub repo → Actions → "Daily Liquidity Update" → "Run workflow"

等几分钟执行完成后，刷新 Vercel 页面就能看到数据了。

## 之后每天自动更新

- GitHub Actions 每天 00:00 UTC (08:00 北京时间) 自动运行
- 运行 pipeline → 生成 latest.json → commit & push
- Vercel 检测到 push → 自动重新部署（通常 < 30s）

## 自定义域名（可选）

Vercel Settings → Domains → 添加你的域名

## 本地调试

```bash
# 跑 pipeline 生成数据
python run_daily.py

# 拷贝到 web
cp output/web/latest.json web/data/latest.json

# 启动本地服务器
cd web && python -m http.server 8080
# 浏览器打开 http://localhost:8080
```

## 未来升级路径

1. **Supabase 后端**: 把 latest.json 存到 Supabase Storage，前端直接读 Supabase URL
2. **Vercel Serverless**: 把 Python pipeline 部署为 Vercel Function (需要精简依赖)
3. **实时推送**: 接 WebSocket 或 Supabase Realtime，数据更新后前端自动刷新
4. **多用户**: 加认证，不同用户看不同 watchlist
