# SHB Web 查询服务 (shb-web)

> **售后宝（SHB）在线报表 Web 服务** — 云端实时查询、周计划/周总结、项目进度、服务人员工作量、调试/保运/验收进度、工单地图/客户地图/人员地图、同步中心、身份管理、账号管理（三级角色）

---

## 🚀 快速启动

### 系统要求
- Python 3.10+
- Flask / Gunicorn / Uvicorn（生产建议 Gunicorn + gevent）
- Nginx 反代（Basic Auth + X-Remote-User 传给后端）
- systemd 托管

### 本地开发
```bash
cd /root/.openclaw/workspace/shb-web
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py                    # 端口 5782，绑定 127.0.0.1
# 访问 http://127.0.0.1:5782
```

### 生产部署（已在线）
| 组件 | 配置 |
|---|---|
| systemd 服务 | `shb-web.service`（Enabled，自启） |
| 监听地址 | `127.0.0.1:5782`（Flask 内置/ Gunicorn） |
| Nginx 反代 | `/shb/` → `http://127.0.0.1:5782/` |
| 认证 | Nginx Basic Auth（`/etc/nginx/.shb-passwd`），用户名即 X-Remote-User 传给后端 |
| 进程 | 1 worker（可扩为 gunicorn -k gevent -w 4） |
| 虚拟环境 | `/root/.openclaw/workspace/shb-web/venv` |

---

## 📂 目录结构

```
shb-web/
├── app.py                 # Flask 主程序（3884 行）
├── online.py              # 在线查询 / 问题工单 / 备注留言 / 同步日志（2989 行）
├── weekly_core.py         # 周计划 / 周总结（141 行）
├── workload_core.py       # 服务人员工作量（97 行）
├── debug_core.py          # 调试/保运/验收 进度（71 行）
├── bridge.py              # MCP / SHB 客户端薄封装（57 行）
├── sync_aitable.py        # 钉钉 AI 表格同步脚本（211 行）
├── templates/
│   └── index.html         # 单页前端（2913 行，Vue-less 原生 JS）
├── static/
│   ├── js/echarts.min.js  # 图表库
│   └── js/china.json      # 中国地图 GeoJSON
├── scripts/
│   └── sync_users.py      # SHB 用户同步（CLI 可独立跑）
├── docs/
│   └── identity-switch-design.md  # 身份切换设计文档
├── permissions.json       # 用户权限（运行时自动写，含 role 字段）
├── .gitignore             # 运行时/敏感文件排除
├── requirements.txt       # 依赖（若有）
└── venv/                  # 虚拟环境（不入库）
```

---

## 🔌 核心 API 端点

| 分类 | 端点 | 方法 | 说明 |
|---|---|---|---|
| **首页** | `/` | GET | SPA 入口（渲染 `templates/index.html`） |
| **健康检查** | `/health` | GET | `{"status":"ok"}` |
| **在线查询** | `/api/online-query` | POST | 统一查询入口（工单/客户/人员/站点/地图） |
| **问题工单** | `/api/online/issue-detail` | GET | 单工单详情（含备注留言组） |
| **同步中心** | `/api/sync` | POST | 触发同步（AI 表格/用户/工单） |
|  | `/api/sync/status` | GET | 同步任务状态 |
|  | `/api/synclog` | GET | 同步日志 |
| **身份管理** | `/api/identity/list` | GET | 列表（序号 / userId / 映射状态） |
|  | `/api/identity/add` | POST | 新增身份（姓名/钉钉ID/profile/手机/备注） |
|  | `/api/identity/authlink` | POST | 生成授权链接（自动发钉钉） |
|  | `/api/identity/authstatus` | GET | 授权轮询 |
|  | `/api/identity/switch` | POST | 切换全局身份 |
|  | `/api/identity/<profile>` | DELETE | 删除身份（二次确认，顺序：移白名单→删映射→删 profile） |
| **账号管理** | `/api/users-account` | GET/POST | 列表/保存（组头全选+半选+计数） |
|  | `/api/users-account/<name>` | DELETE | 删除用户 |
| **导出** | `/api/export` | POST | CSV 导出（仅 admin/subadmin 可见且有 export=true） |
| **审计** | `/api/audit` | GET | 操作审计日志 |
| **Webhook** | `/api/web-audit` | POST | 外部审计回调 |

> 前端路由均为**绝对路径** `/shb/api/...`（由 nginx `location /shb/` 反代去掉 `/shb` 前缀转发到 5782）。

---

## 🔐 权限模型（三级角色）

```
admin（内置，全量）  >  subadmin（子管理员，受限）  >  user（普通用户）
```

### admin
- 内置用户名：`admin`（硬编码 `UA_ADMIN = "admin"`）
- 权限：全 Tab、导出、用户增删、密码重置、角色改动、身份管理全权、同步中心、切换任意身份

### subadmin（子管理员）
- `permissions.json` 写 `"role": "subadmin"`（无 role = 普通用户）
- **能**：
  - 调整**普通用户**的 Tab 勾选与导出开关
  - 权限范围**限于本人拥有的 Tab**（组头全选、半选、计数同步）
  - 查看/操作：同步中心、同步日志、身份管理（含切换身份、删除身份）
- **不能**：
  - ❌ 新增/删除用户
  - ❌ 重置密码
  - ❌ 修改角色（subadmin/user/admin 互改）
  - ❌ 碰 admin 或其他 subadmin 的权限（只读）
  - ❌ 修改自己的权限

### user（普通用户）
- 只看、只查、只导出（被授予的 Tab）
- 无任何管理入口

### 权限文件结构
```json
{
  "admin": {"tabs": ["*"], "export": true, "role": "admin"},
  "张三": {"tabs": ["ol:issue", "bk:customer"], "export": false, "role": "subadmin"},
  "李四": {"tabs": ["st:info"], "export": false}
}
```
- `*` = 所有 Tab
- `bk:*` = 所有备份 Tab
- Tab key 与侧边栏 7 组完全对应（概览/工单在线查询/工单备份/客户备份/在线报表/本地报表/系统）

---

## 🆔 身份管理（多钉钉账号）

### 概念
- **Profile**：一个钉钉账号的完整凭证（token + cookie + userId）
- **Mapping**：`身份标识(profile名) → 钉钉 userId`（文件 `identity_dingtalk.json`）
- **Whitelist**：钉钉回调白名单（`allowFrom`/`groupAllowFrom`，**必须为字符串**，数字会丢前导零/精度）

### 关键流程
1. **新增** `/api/identity/add`：一次完成「建占位 profile → 写 mapping → 进白名单」
2. **授权** `/api/identity/authlink`：生成链接 → **自动发钉钉 Markdown** `[点此完成授权](url)` → 用户点击 SSO → `/api/identity/authstatus` 轮询 → 成功后**只刷新列表，不切换全局身份**
3. **切换** `/api/identity/switch`：**仅手动点击**，「授权 ≠ 切换」
4. **删除** `/api/identity/<profile>`：手输 profile 名二次确认 → 移白名单 → 删 mapping → 删 profile 文件+孤立令牌

### 映射状态（列表徽章）
| 状态 | 含义 |
|---|---|
| ✅ 正常 | profile 有效、userId 在白名单、类型为字符串 |
| ⏳ 待授权 | 占位 profile，token 为空 |
| ⚠️ 未配置钉钉ID | mapping 缺 userId |
| ❌ 白名单缺失 | userId 不在 `allowFrom`/`groupAllowFrom` |
| ⚠️ 白名单类型异常 | 白名单里该 ID 被写成了数字 |
| 👻 幽灵映射 | profile 存在但 config.json 无此 profile（fail closed） |

> 🔴 **授权 ≠ 切换** — 授权成功只更新该 profile 的令牌；全局 `current_profile` 只能手动点「切换」改变。

---

## ⚙️ 配置文件

| 文件 | 路径 | 说明 |
|---|---|---|
| 权限配置 | `./permissions.json` | 运行时自动读写，含 role/export/tabs |
| 身份映射 | `/root/.openclaw/workspace/shb-mcp/identity_dingtalk.json` | **唯一真相源**，mtime 热加载，MCP 与 Web 共用 |
| Nginx 密码 | `/etc/nginx/.shb-passwd` | `chown root:www-data; chmod 640`；`htpasswd -b` 写入 |
| 白名单 | OpenClaw config (`mcp.servers.shb.allowFrom/...`) | **必须字符串**，数字会导致前导零/精度丢失 |
| MCP 配置 | `shb-mcp/shb_mcp_server.py` 读 `identity_dingtalk.json` | 热加载，增删人员无需重启 gateway |

---

## 🛠 运维操作

```bash
# 重启服务
systemctl restart shb-web.service

# 查看日志
journalctl -u shb-web -f -n 100

# 语法检查
cd /root/.openclaw/workspace/shb-web && python3 -m py_compile app.py online.py ...

# 同步用户（CLI）
cd /root/.openclaw/workspace/shb-web && python3 scripts/sync_users.py

# 权限文件手动改（不推荐，用页面）
vim permissions.json
```

---

## ❓ 常见问题

| 现象 | 原因 | 解决 |
|---|---|---|
| 私聊被拦截、授权链接点击无反应 | 白名单 `allowFrom` 写成**数字**（丢前导零/精度） | `openclaw config patch --stdin --replace-path` 恢复为字符串数组 |
| 授权成功后莫名切换了全局身份 | 旧版 bug：`idPoll` 成功后调了 `idSwitch` | 已修复：授权只 `fetchIdentity()`，切换仅手动 |
| 身份列表全显示"未配置钉钉ID" | `identity_dingtalk.json` 放错目录（应在 `shb-mcp/`） | 两处读同一文件才行 |
| 子管理员保存后权限丢失 | 超出范围的原有 Tab 被静默剥离 | 已修复：范围外原有 Tab 自动保留 |
| 问题工单只出 3 种状态 | 前端硬编码漏了「最终审核/分配相关部门」 | 已补齐 5 种，默认勾 4 个未完结 |

---

## 📝 变更记录索引

| 版本 | 日期 | 核心变更 |
|---|---|---|
| v1.8.1 | 2026-11-23 | 三级角色、身份管理增强、授权自动发钉钉、删除身份、Tab 分组 UI、问题工单备注留言、授权≠切换修复、白名单字符串化修复 |
| v1.8.0 | 2026-11-20 | 导出权限可配置、四组在线报表改造 |
| v1.7.1 | 2026-11-18 | 工单查询按类型并发、稽核检查默认当天 |
| v1.7.0 | 2026-11-17 | 工单管理看板、三张地图页、用户列表换接口 |
| v1.6.0 | 2026-11-13 | 工单在线查询与报表修复 |
| v1.5.0 | 2026-11-11 | 周计划/周总结上线、多用户改造 |


---

## 📄 相关文档

- **全量设计/部署/权限/坑位**：`/root/.openclaw/workspace-shb/docs/SHB.md`（SHB 总仓库 `docs/SHB.md`）
- **身份切换设计**：`docs/identity-switch-design.md`
- **钉钉知识库备份 SOP**：`skills/dingtalk-wiki-backup/SKILL.md`
- **员工授权 SOP**：`skills/shb-agent/SOP.md`（`shb-agent` 文件夹）

