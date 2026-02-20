# 艺术史复习 App

## 功能
- 逐张浏览图片（支持键盘左右键）
- 点击图片查看详情并编辑：年份、时期、作者、生产地、地区、风格、材质、历史背景（中英对照）
- 分类系统：可给每张图添加自定义分类标签并筛选
- 支持按条目类型筛选：`作品` / `参考图`
- 两图比较：任意选两张图，写下区别并本地保存
- 内置可下载对比表格：`app/data/comparison_table.csv`
- 历史背景支持联网来源细分，并在详情里显示来源链接
- 条目自动区分 `artwork` 与 `reference`（课程背景图/说明图）

作者字段规则：
- 若 PPT 中未能可靠抽取作者，自动使用 `生产地 + artist` 作为兜底值。

## 启动
在项目根目录执行：

```bash
python3 -m http.server 8000
```

然后打开：

- <http://localhost:8000/app/index.html>

## 数据重建
如果你更新了 PPT 文件，可重新生成图片和数据：

```bash
python3 scripts/build_dataset.py
```

生成文件：
- `app/data/artworks.json`
- `app/data/comparison_table.csv`
- `app/assets/*`

说明：
- 年份和时期仅保留作品创作相关信息；如果源 slide 无法明确判断，会留空。
- 元数据优先来自 PPT 原始文本抽取。你可以在 app 里点击图片后手动修正并保存。
- `comparison_table.csv` 额外包含 `record_type` 与 `historical_background_sources` 列，便于筛选对比与追溯来源。
