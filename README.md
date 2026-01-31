## zaimanhua

顺序从 `id=4` 开始递增抓取：

`https://m.zaimanhua.com/api/app/v1/comic/detail/{id}?_v=15`

并将响应 JSON 中 `data.data` 的漫画数据落地到本地 SQLite，供后续检索使用。

保存前会清洗并丢弃以下字段，并额外写入 `aliases`（从网页 keywords 解析出的别名列表）：
`chapters`、`dh_url_links`、`url_links`、`last_updatetime`、`last_update_chapter_name`、`last_update_chapter_id`、
`hot_num`、`hit_num`、`uid`、`subscribe_num`、`corner_mark`、`status`、`comicNotice`、`authorNotice`。

如需清洗历史数据（可选）：使用 `--clean-existing` 触发（同一清洗版本只会执行一次）。

### 随机休息

每次请求循环结束后会随机休息 `100ms~1s`（并可叠加 `--delay` 额外延迟）。

### 并发抓取

- `--aliases-workers`：aliases 并发抓取线程数（默认 1）
- `--detail-workers`：详情接口并发抓取线程数（默认 1，独立设置）

例如：

```powershell
uv run python main.py --aliases-workers 4 --detail-workers 4
```

### 使用

```powershell
uv sync
uv run python main.py
```

可选参数：

```powershell
uv run python main.py --help
```

默认数据库位置：`data/zaimanhua.sqlite3`  
断点续跑：`meta` 表中的 `next_id`（最后一次成功抓取的下一个 id）  
停止条件：
- 连续错误达到 `--max-errors`（默认 5，重试使用指数退让）
- 连续 `--max-zero-streak` 次命中 `data.id=0`（默认 10）  
- 若 `data.id!=0` 但 `title` 为空：跳过保存并记录到 `errors`（不计入连续错误）  
错误记录：`errors` 表（包含错误原因与返回文本）

### 并发运行提示

SQLite 允许多进程并发读，但同一时刻只能有一个写入者；当你同时运行多个实例时会产生写入竞争。
程序已设置 `busy_timeout=60s`，遇到锁会等待而不是立刻报 `database is locked`。

