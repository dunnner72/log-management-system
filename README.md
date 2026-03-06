# 日志管理系统

一个可直接运行的轻量级日志管理系统，使用 `Python 标准库 + SQLite + 原生 HTML/CSS/JS` 构建，不依赖第三方包。

## 功能

- 新增日志：支持 `DEBUG / INFO / WARN / ERROR`
- 日志查询：支持关键字、级别、来源、时间范围筛选
- 日志统计：展示总数、错误数、告警数、主要来源
- 日志删除：支持单条删除
- 数据导出：支持按当前筛选条件导出 CSV
- 本地持久化：日志保存到 `logs.db`

## 运行方式

确保本机安装 Python 3.10+，在项目目录执行：

```bash
python server.py
```

启动后访问：

```text
http://127.0.0.1:8000
```

## 项目结构

```text
.
├── server.py
├── logs.db                # 首次运行后自动生成
├── static
│   ├── app.js
│   ├── index.html
│   └── styles.css
└── tests
    └── test_server.py
```

## 运行测试

```bash
python -m unittest tests/test_server.py
```

## 后续可扩展方向

- 增加用户登录和权限控制
- 支持批量导入日志
- 增加分页、排序和归档策略
- 对接真实业务服务的日志采集接口
