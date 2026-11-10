# shb-web 身份管理（切换 SHB 身份）设计稿

> 状态：设计待确认（2026-10-23 hunter 同意方向 + 指定放入「系统」组）
> 原则：只改默认身份这一个字段，业务脚本 0 改动

## 1. 页面位置

系统组 → **身份管理**（tab key: `identity`），仅 admin 可见。

改动点（3 处，均为登记类）：
- `app.py` `SYSTEM_TITLES` 加 `"identity": "身份管理"`
- `templates/index.html` `groupOf()` 加 `if (key === 'identity') return '系统';`
- 权限文件 `permissions.json` 的 admin.tabs 加 `identity`

## 2. 状态机

| 状态 | 含义 |
|---|---|
| `active` | 当前生效身份（config.json 的 current_profile） |
| `probing` | 正在探测目标身份 token 是否有效 |
| `need_auth` | 目标身份 token 无效，需要授权 |
| `auth_waiting` | 已生成授权链接，等待本人确认（最长 30 分钟） |
| `applying` | 正在写入配置 + 校验 |
| `done` | 切换成功 |
| `failed` | 切换失败，已回滚到原身份 |
| `expired` | 链接超时/过期，可重发或取消 |

流转：

```
active --选身份+点切换--> probing
probing --valid=true--> applying --校验通过--> done（current_profile=新身份）
probing --valid=false--> need_auth --生成链接--> auth_waiting
auth_waiting --本人确认+轮询成功--> applying --> done
auth_waiting --超时30m/链接过期--> expired（可重发/取消）
applying --校验失败--> failed（回滚原身份）
任意状态 --取消--> 回到 active（原身份不变）
```

关键约束：**授权成功才写 current_profile**（原子切换），避免等待期全系统用坏身份跑。

## 3. 后端接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/identity/list` | 列出全部 profile：显示名、最后登录时间、当前标记 |
| POST | `/api/identity/switch` | body `{profile}`：先探测；有效→直接切换；无效→返回 need_auth |
| POST | `/api/identity/authlink` | body `{profile}`：起后台进程生成链接，返回 URL + 状态 |
| GET | `/api/identity/authstatus` | query `profile`：轮询 waiting/success/timeout |
| POST | `/api/identity/cancel` | 取消等待中的授权进程 |

全部 admin 限定；写操作记操作日志。

## 4. 切换动作（核心）

1. 备份 `~/.config/shb-cli/config.json` + `credentials.json`（按 SOP，文件名带日期）
2. 探测：`shb-cli --profile X config` → 判断 `valid: true`
3. 有效 → 原子写 `current_profile = X`（临时文件 + rename）
4. 复验：`shb-cli --profile X config` 再跑一次 + 可选轻量业务探测（如 `customer list --page-size 1`）
5. 失败 → 回滚 `current_profile` 为原值
6. 记操作日志：操作人、从谁到谁、结果、耗时

## 5. 授权链接（不动 current_profile）

- 命令：`SHB_MCP_GUARD=1 shb-cli auth login --profile X --env dingtalk --no-browser --timeout 30m`
- `--no-browser` 适配无浏览器服务器，**不需要 VNC**
- 链接一次性，只发被授权人本人（走 SOP），管理员不代点
- 后台进程 + 状态轮询；同一时刻只允许一个等待中的授权任务

## 6. 并发与锁

- 文件锁 `/tmp/shb_identity_switch.lock`
- 同一时刻只允许一个 `auth_waiting`；并发点击直接拒绝并提示

## 7. 两个按钮必须分开（SOP 约束）

| 按钮 | 作用 | 是否改默认身份 |
|---|---|---|
| 切换身份 | 让全系统用这个身份跑 | 是 |
| 生成授权链接 | 给员工个人用（MCP 那条线） | 否 |

SOP 原文要求「授权不应切换默认身份」，界面上必须显式区分，否则运维会互相打架。

## 8. 不改动的部分

- 业务脚本、同步脚本、定时任务：0 改动（不传 `--profile` 自动跟随默认身份）
- MCP（`shb-server`）：保留现状「钉钉用户 → 个人身份」映射（hunter 2026-10-23 定 B 方案）

## 9. 风险与提示

- 切换后数据可见范围会变（普通员工身份看得到的工单更少），界面需提示「当前身份 + 权限范围」
- token 过期是主要故障源，靠「探测优先 + 授权兜底」拦截
- 单点登录限制：同一 SHB 账号别处登录会顶号（报 2016），需重新授权

## 10. 工作量

- 后端：读/写配置、探测、授权进程管理、日志 ≈ 80-100 行
- 前端：一页（列表 + 切换 + 授权链接 + 状态轮询）≈ 80-100 行
- 业务脚本：0 行
