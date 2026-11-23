# aiops-log-copilot 工作流文档

本文档按**实际执行顺序**串联项目核心文件，分为三个板块：
1. 简短工作流全景图
2. 每个节点的详细说明
3. 配置和测试文件汇总

> **维护约定**：每当项目新增文件或流程变化时，同步更新本文档。判断"一个文件属于哪个节点"的标准是——这个文件里定义了流程中的哪个关键动作。

---

## 第一板块：工作流全景图

```
容器/文件里的原始字节
    │
    ▼
【节点 0】取数 —— collect/adapters/base.py + file.py + docker.py
    │  把字节变成 (line_no, text) 流，喂给节点 1
    │  base.py 定义协议 + 核心编舞者 ComponentReader
    │  file.py 从 bind mount 读磁盘 → 路径 A
    │  docker.py 从 docker exec 读容器内文件 → 路径 B
    │  两条路径产出的字节必须完全一致（V4 红线）
    ▼  (collect/adapters/base.py + file.py + docker.py)
【节点 1】翻译 + 脱敏
    │  把原始文本行翻译成 LogRecord 对象（canonical.py）
    │  三步：剥前缀抠字段 → 续行拼接 → 隐私擦除（redact.py）
    │  failed 行绝不丢弃，原文留 raw
    ▼  (collect/canonical.py + collect/redact.py)
【节点 2】LogRecord 构造校验
    │  canonical.py 构造 LogRecord 时自动触发
    │  domain/log.py 的 frozen dataclass 在 __post_init__ 里拦截非法值
    ▼  (domain/log.py)
【节点 3】组装元信息 + 标注
    │  构建 CaseManifest / Labels 结构
    │  描述"这个病例包是什么、从哪来、算不算有效"
    ▼  (domain/case.py)
【节点 4】创建病例包目录
    │  建目录结构，写空 logs.jsonl / labels.json
    │  不写 manifest（那是最后一步的事）
    ▼  (store/casepack.py → create_pack)
【节点 5】流式追加写日志
    │  采集过程中一条条 append 到 logs.jsonl
    │  中途崩了也不丢已采的行
    ▼  (store/casepack.py → LogWriter)
【节点 6】finalize 收尾（最关键的一步）
    │  读回所有日志 → 排序 → 检查 id 重复
    │  重算 counters、两个哈希、写 anchors
    │  最后写 manifest.json ← 写完才算这个 pack 完成
    ▼  (store/casepack.py → finalize)
【节点 7】pack-lint 开箱检查
    │  结构齐不齐、计数对不对、哈希能不能重算
    │  正常例零 ERROR、故障例有没有采到证据
    ▼  (scripts/pack_lint.py → audit / audit_levels)
【节点 8】读回供下游使用
    │  P-B/P-C 用 iter_logs / read_manifest 等函数读
    │  整个项目只有这一套读逻辑，不会各自漂移
    ▼  (store/casepack.py → read_*)
```

**三层架构**：
- **collect 层**（节点 0–1）—— 取数 + 翻译 + 脱敏，把外部世界的字节转成内部标准对象
- **domain 层**（节点 2–3）—— 模型层，只管字段合不合法，不知道磁盘和网络
- **store 层**（节点 4–8）—— 存储层，只管字节怎么进出磁盘

三层完全解耦：collect 产出 domain 对象，store 消费 domain 对象，domain 自己不依赖任何一层。

---

## 第二板块：节点详解

### 节点 0：取数 —— `collect/adapters/base.py` + `file.py` + `docker.py`

**一句话**：日志采集的"水管总表"——`base.py` 定义协议和核心调度器，`file.py` 从 bind mount 读磁盘（路径 A），`docker.py` 从 `docker exec` 读容器内文件（路径 B）。两条路径产出的字节**必须完全一致**，这是 V4 红线。

**为什么先有协议、后有实现**：P-A 要两条**完全等价**的取数路径。V4 的红线是"两条路径产出的 `canonical_sha256` 必须相等"——翻译成代码就是：**流水线后半段（canonicalize / package）一行都不许知道日志是从哪来的**。

#### base.py 做了什么（6 个核心部件）

| 部件 | 作用 |
|---|---|
| **StreamSpec** | 一条物理日志流的"身份证"——属于哪个组件（`nn/dn1/dn2/dn3`）、叫啥名（`main/gc/audit/stdout`）、读取先后顺序（写死 `order`，不靠目录遍历或文件时间）、物理路径 |
| **StreamReader（Protocol）** | "怎么从一条流里读"的接口——`FileReader` 读磁盘、`DockerReader` 跑 `docker exec tail -c`，各自实现这个 Protocol |
| **SourceAdapter（Protocol）** | "一种取数方式"的接口——`streams()` 列出所有流、`open(spec)` 打开一条流。file 和 docker 各有一个实例 |
| **ComponentReader** ⭐ | **最核心的编舞者**——把一个组件的多条流（主日志 + GC + audit）按 `StreamSpec.order` 顺序依次读完，编成**一条连续编号**的行流喂给 canonical.py。没有它 id 会在多条流之间撞号 |
| **LineAllocator** | 给多条流分配连续 `line_no`——起点是"checkpoint 里所有流的最大值 + 1"，不是每条流自己从 1 起 |
| **CheckpointMismatch + line_sha1** | 续采安全双重校验——inode 变了（日志轮转、容器重建）或 offset 前最后一行 sha1 对不上（文件被截断或改写）都抛异常，绝不"猜着接着读" |

#### file.py（路径 A：bind mount 读磁盘）

**物理基础**：Hadoop 容器的 `/opt/hadoop/logs` 通过 bind mount 落到 `runtime/hdfs-logs/<container>/`——同一份日志既是容器里的文件也是宿主机上的文件。

| 类 / 函数 | 作用 |
|---|---|
| **FileStream** | 一条流的"声明"——用 glob pattern（`hadoop-hdfs-datanode-*.log`）定位，hostname 用 `*` 挡住。不准写 `*.log*`（会命中轮转的 `.log.1`） |
| **hdfs_streams()** | 预定义的 Hadoop 默认日志布局：每个组件 main → audit → gc → stdout，order 写死 |
| **FileAdapter** | `SourceAdapter` 实现——`streams()` 解析 glob 产出 StreamSpec，`open(spec)` 创建 FileReader |
| **FileReader** | `StreamReader` 实现——`seek(offset)` + `readline()` 按字节读。半行不消费、轮转/截断抛异常、文件不存在跳过 |
| **_decode_line()** | 一行字节 → 文本：去 `\n` → 去 `\r` → UTF-8 decode 用 `errors="replace"` |
| **sha256_file_head()** | 读文件前 N 字节的 sha256——`input_fingerprint` 的原料 |

#### docker.py（路径 B：docker exec 读容器内文件）

**为什么不用 `docker logs`**：`docker logs` 只能按时间戳 `--since`，给不出字节偏移——续采会变成"猜时间戳"，V3（不重不漏）立即失守。所以这里**把 docker 当作远端文件系统**，用三条命令模拟文件操作：

| 本地文件操作 | docker.py 等价命令 |
|---|---|
| `os.stat()`（拿 inode / size） | `docker exec <c> stat -c '%i %s' <path>` |
| `seek(offset)` + 逐行读 | `docker exec <c> tail -c +<offset+1> <path>` |
| 读 offset 前一行做校验 | `docker exec <c> sh -c "tail -c +<start> <path> \| head -c <len>"` |

**`tail -c +N` 是 1 起的字节位置**，所以偏移 `offset` 对应 `+offset+1`——这就是 FileReader 和 DockerReader 能**逐字节对齐**的秘密。

| 类 / 函数 | 作用 |
|---|---|
| **SubprocessRunner** | 执行 `docker` 命令的 runner——有 60 秒超时，防止容器卡死时采集器挂住。抽成可注入的，测试不依赖真 Docker |
| **DockerStream** | 一条流的声明——与 FileStream 唯一区别是 pattern 不带子目录（docker 适配器一个实例只服务一个容器） |
| **container_streams()** | 一个容器的默认布局——main → audit（只有 nn 有）→ gc → stdout，和 hdfs_streams() 一一对应 |
| **DockerAdapter** | `SourceAdapter` 实现——`streams()` 通过 `docker exec ls` 列目录再 fnmatch 匹配，`open(spec)` 创建 DockerReader |
| **DockerReader** | `StreamReader` 实现——**与 FileReader 逻辑逐字对应**（抄不是偷懒，是要求）：同样的三重校验、同样的半行跳过、同样的 line_sha1。唯一区别是数据来源换成了 `tail -c +N` |
| **DockerCommandError** | docker 工具链坏了（容器不存在、docker 没装、超时）——和 CheckpointMismatch 区分开（后者是数据不可信，重试没用；前者重试可能有用） |

**stdout 为什么不单独走 `docker logs`**：Hadoop 的 stdout 基本为空，而且 `docker logs` 无法按字节续采。第 12 步的 compose 里让 stdout 也落成文件（`... 2>&1 | tee /opt/hadoop/logs/<component>.out`）——**一个 adapter 只有一种取数语义**，比多一个"特例"更稳。

#### ComponentReader 工作流（两条路径通用）

```
ComponentReader(adapter, "dn1", checkpoint)
    │
    │  按 StreamSpec.sort_key 排序，得到:
    │    dn1:main (order=0) → dn1:audit (order=1) → dn1:gc (order=2) → dn1:stdout (order=3)
    │
    ▼
for spec in sorted_specs:
    reader = adapter.open(spec)          # file.py → FileReader(spec)
                                         # docker.py → DockerReader(spec, container, runner)
    for _phys_no, text in reader.read(): # file: seek(offset)+readline()
                                         # docker: tail -c +<offset+1>
        line_no = allocator.take()       # 分配全局连续编号
        yield line_no, text              # 喂给 canonicalize()
    checkpoint()                         # 记录 inode + offset + tail_line_sha1
```

#### 续采三重校验（FileReader 和 DockerReader 逐字相同）

上次采到 offset=5000，inode=12345，tail_line_sha1=abc123 → 这次续采：

```
① inode 变了？
   12345 ≠ 当前 inode → 文件被轮转/容器重建 → 抛 CheckpointMismatch

② offset > 文件当前大小？
   5000 > size → 文件被 truncate → 抛 CheckpointMismatch

③ offset 前最后一行 sha1 对不上？
   tail_before(5000) ≠ abc123 → 文件被改写过 → 抛 CheckpointMismatch
```

三个都对得上才继续读。**半行永远不消费**——末尾没 `\n` 就当这行不存在，offset 停在它前面。

#### 两条不变式（适配器必须守住）

| 不变式 | 直白解释 |
|---|---|
| **不重不漏** | 续采三重校验全过才继续。猜错要么读重了要么读漏了，V3 直接挂 |
| **编号与 adapter 无关** | StreamSpec.order 写死了顺序，ComponentReader 按读入顺序给全局编号。所以 file 和 docker **一定产出同一套 line_no**——这是 V4 的前提 |

**产出**：`iter[(line_no, text)]` —— 连续编号的原始行流，直接喂给 `canonicalize()`。

---

### 节点 1：翻译 + 脱敏 —— `collect/canonical.py` + `collect/redact.py`

**一句话**：日志采集的"翻译官 + 隐私擦除器"——把容器里读出来的原始文本行，翻译成项目内部的标准 `LogRecord` 对象，同时把敏感信息替换成占位符。

整个节点内部是有序的**三步流水线**，全部在 `canonicalize()` 这一次调用里完成：

```
原始文本行
  ↓ 第一步：解析（canonical.py _parse_head）
剥前缀 → 抠时间戳 → 抠级别 → 抠 logger → 抠正文
  ↓ 第二步：续行归并（canonical.py 主循环）
下一行不是时间戳开头 → 拼到上一条后面（正文用 \n 保留换行）
  ↓ 第三步：脱敏（redact.py PatternRedactor）
message 和 raw 里的 IP/主机名/路径/密钥 → 占位符
  ↓
LogRecord 对象（过 log.py 构造校验）
```

#### canonical.py 负责前两步

- **组件名归一**（`normalize_component`）：容器里叫 `aiops-lab-dn1` 或 `hdfs-datanode2`，统一改成闭集短名 `dn1` / `dn2`。认不出来的绝不乱猜，整条进 failed
- **逐行解析**（`_parse_head`）：剥掉旧数据的 `[component] ` 前缀 → 从行首抠时间戳 → 抠级别（Hadoop 的 `WARNING` 也收成 `WARN`）→ 抠 logger → 抠正文。三种真实日志格式都能吃
- **续行归并**（`canonicalize` 主循环）：下一行不是时间戳开头 → 说明是上一条日志的续行（比如 Java 异常栈）→ 拼到上一条 `_Group` 后面
- **脱敏注入口**（`Redactor` Protocol）：第三步脱敏通过这个 Protocol 注入。没装脱敏器时产出会带真实 host/IP，只允许在测试和 dry-run 里用
- **窗口校验**（`assert_within_window`）：所有时间戳落在采集窗口 ±1 分钟内，超出直接报错（静默丢行会让 V2 的 132/132 变成"看起来对"）

#### redact.py 负责第三步

- **四类规则**（执行顺序 ip → token → host → path）：

  | 规则 | 匹配什么 | 占位符 |
  |---|---|---|
  | ip | IPv4 四段数字（逐段限制 0–255） | `<:IP:>` |
  | token | 独立密钥（sk-、ghp_、AKIA、JWT）和 `key=value` 形态（键名保留） | `<:TOKEN:>` |
  | host | localhost、Windows 机器名、容器名、带 TLD 的域名 | `<:HOST:>` |
  | path | 绝对路径（`/` 或 `C:\` 开头） | `<:PATH:>` |

- **幂等保证**（最精巧的部分）：先把**已有**占位符换成 `\x00` 哨兵 → 跑规则（规则输出也写哨兵）→ 最后统一换回 `<:XXX:>`。两次跑输出完全相同——否则旧 132 例日志早就 `<:IP:>` 满屏了，再脱一次 canonical_sha256 会变
- **计数留账**：每条规则命中几次都返回，最终落进 `manifest.counters.redacted`。脱敏是"看不见的过滤器"，没计数就没人能发现规则失效了
- **宁可少脱，不可错脱**：规则取窄——IP 必须四段合法、路径必须以分隔符开头。`org.apache.hadoop...` 里的短点分词不能被当成域名脱掉

#### 三个硬规则（canonical.py）

| 规则 | 为什么 |
|---|---|
| failed 行绝不丢弃 | 一行都认不出来时仍产出 LogRecord，原文留 raw。丢弃 = 静默少数据，比留下坏行更危险 |
| 续行判据在"剥掉前缀之后的 body"上做 | 旧数据每一行（包括续行）都带 `[aiops-lab-dn1] ` 前缀，拿原始行首判时间戳会把每一条续行都认成新日志 |
| 时间戳一律按 UTC 处理，不猜时区 | 猜时区就是"把 UTC 当本地时间差 8 小时"事故的入口，manifest.tz 已固定为 UTC |

**产出**：`CanonicalResult(records, raw_line_count, redacted)` —— 归并后的 LogRecord 元组、原始物理行数、脱敏命中计数。

---

### 节点 2：LogRecord 构造校验 —— `domain/log.py`

**一句话**：日志行的"质检台"。canonical.py 在构造 LogRecord 时自动触发，任何非法值当场拦截。

**做了什么**：
- 定义 `LogRecord` dataclass——规定一条日志必须有哪 10 个字段（id、ts、level、component、logger、message、source、raw、parse_status、template_id）
- 构造时就校验所有不变式：
  - level 必须是 TRACE/DEBUG/INFO/WARN/ERROR/FATAL（写 WARNING 会直接报错）
  - ts 必须带时区且偏移为 0（UTC），防"把 UTC 当本地时间差 8 小时"
  - id 必须形如 `dn2:00123`，而且前缀必须等于 component
  - parse_status=ok 时 raw 必须为 None（原文在 anchors 里留着，不重复存）
- 提供 `to_json_line` / `from_json_line` 做 jsonl 往返序列化
- 提供 `canonical_payload` 取 6 个关键字段，给后续算哈希用

**为什么重要**：
校验全塞在 `__post_init__` 里，错数据**构造时就炸**，绝不会流到下游。canonical.py 产出的 LogRecord 每条都必须过这一关，是整个数据管道的第一道闸门。

---

### 节点 3：组装元信息 —— `domain/case.py`

**一句话**：病例的"档案柜"。定义 manifest.json 和 labels.json 两个文件的全部结构。

**做了什么**：
- 定义多个子部件 dataclass：
  - `SourceRef` — 每条日志从哪个容器/文件采的
  - `Window` + `PhaseWindow` — 采集时间段 + baseline/fault/recovery 三段标注
  - `Counters` — 原始行数、归并行数、按级别/组件的计数
  - `Hashes` — 两个哈希（input_fingerprint 只认源、canonical_sha256 认内容）
  - `Checkpoint` / `AdapterState` — 续采进度快照
  - `Origin` — 新采的还是从旧项目迁过来的
  - `Labels` + `FaultInjection` + `Expect` — 人填的根因/严重度/期望值、机器填的故障注入记录
- 组装成两个总装类：`CaseManifest`（manifest.json）和 `Labels`（labels.json）
- 每个部件同样在 `__post_init__` 里做严格校验

**与 log.py 的关系**：
导入了 log.py 的 `COMPONENTS`、`LogLevel`、枚举收窄、时间格式化等工具——"什么算合法 component"只能有一个定义，两份迟早会漂。

---

### 节点 4：创建病例包目录 —— `store/casepack.py` → `create_pack()`

**一句话**：在磁盘上开一个新柜子。

**做了什么**：
- 检查 case_id 和 labels 里的 case_id 对得上
- 检查没有现成的完成 pack（manifest.json 存在就拒绝覆盖——重采应该换 case_id，而不是把上一份证据抹掉）
- 创建目录 + `anchors/` 子目录
- 先 `touch` 空的 logs.jsonl 和 metrics.jsonl（后续全是 append 写，不用再关心"文件在不在"）
- 用原子写写 labels.json
- **不写 manifest**——这是最后一步的事

**为什么不写 manifest**：
manifest 在这个项目里就是"完成标记"。采集途中崩了，目录里没有 manifest，读的人一看就知道是未完成 pack，不需要额外状态位。

---

### 节点 5：流式追加写日志 —— `store/casepack.py` → `LogWriter`

**一句话**：采集过程中逐条往柜子里塞病历正文。

**做了什么**：
- 打开 logs.jsonl 用 `a` 模式追加
- `append(record)` 写一条，`extend(records)` 批量写
- 统一用 `\n` 作为行尾（Windows 默认的 `\r\n` 会让 canonical_sha256 跨机器不一致）
- 提供 `flush()` 强制落盘（注入脚本执行前调一次，崩溃时也有迹可循）
- 支持上下文管理器 `with open_log_writer(paths) as writer:`

**为什么流式追加而不是攒内存**：
采一例是 20 分钟，中途 Ctrl-C 或容器崩了很常见。流式追加意味着**已经采到的行不丢**，重启后接着采——这是续采不重不漏的前提。

---

### 节点 6：finalize 收尾 —— `store/casepack.py` → `finalize()`

**一句话**：采集结束后的"封箱"动作，是整个流程里最关键的一步。

**执行顺序**：
1. 读回所有 logs.jsonl 里的记录
2. 按 `(ts, id)` 排序（采集时是到达序，此时才排）
3. 检查 id 有没有重复（续采时 line_no 算错了就会撞）
4. 写临时文件 → `os.replace` 原子重写 logs.jsonl
5. **重算 counters**——不信任调用方报的数，数实际写出的记录
6. **算 canonical_sha256**——只覆盖 6 个字段（排除 source/raw/parse_status/template_id）
7. **算 input_fingerprint**——只覆盖源信息，不含任何元数据
8. 写 anchors/index.json（原文指针）
9. **最后写 manifest.json** ← 这一步算封箱完成

**为什么两个哈希要分开**：
- `canonical_sha256` 变了 + `input_fingerprint` 没变 → **代码改了**
- 两个都变了 → **源数据变了**
- `input_fingerprint` 不含 pipeline_version/redact_policy/canonical_profile，否则"换了个版本号"会被误判成"数据变了"

---

### 节点 7：pack-lint 开箱检查 —— `scripts/pack_lint.py`

**一句话**：柜子封箱后的"验收"——这个 pack 能不能被当作有效样本用。

**调用方式**（全部通过 Makefile）：
```bash
make pack-lint                                     # data/cases 下全部
make pack-lint ARGS="--case inc_dnvol_01"          # 只校验一个
make pack-lint ARGS="--check levels"                # 额外按 V5/V6 验收级别
```

**V1 结构检查**（`store/casepack.py` → `audit()`）：
- 文件齐不齐（manifest、logs、labels、anchors/index.json）
- 能不能正常读回
- labels.case_id 和 manifest.case_id 对不对得上
- id 有没有重复、时间戳有没有乱序
- counters 和盘上实际行数对不对得上（lines、raw_lines、by_level、by_component、parse_failed）
- canonical_sha256 重算一遍和 manifest 里存的对不对得上

**V5/V6 级别检查**（`store/casepack.py` → `audit_levels()`）：
- 正常例（root_cause=null）→ 不应该有 ERROR
- 故障例（有 root_cause）→ 必须满足 labels.expect 里的期望值（最少 ERROR/WARN 行数、ERROR 应该出现在哪些组件上）

**为什么返回问题列表而不是抛异常**：
pack-lint 要一次列出一个 pack 的所有问题。抛异常只能报第一个，人修数据时最不想"改一条、跑一次"。

---

### 节点 8：读回供下游使用 —— `store/casepack.py` → `read_*` 系列

**一句话**：下游模块（P-B/P-C）读病例时**唯一**应该调用的地方。

**读函数**：
| 函数 | 作用 |
|---|---|
| `is_complete(paths)` | 判断 manifest 在不在（完成没） |
| `read_manifest(paths)` | 读 manifest.json |
| `read_labels(paths)` | 读 labels.json |
| `iter_logs(paths)` | 逐行生成 LogRecord（流式，大数据量友好） |
| `read_logs(paths)` | 一次性读成 list（小数据量方便） |
| `read_anchors(paths)` | 读 anchors/index.json |

**异常体系**（统一用 `CasePackError` 的子类）：
- `CasePackIncomplete` — 目录存在但 manifest 没写（采集未完成）
- `CasePackExists` — 已经有完成的 pack，拒绝覆盖
- `CasePackCorrupt` — 盘上内容读不出来

**为什么强调"唯一调用点"**：
P-B / P-C / P-E 各写一遍读逻辑，就是三份会各自漂移的解析器。统一走 casepack.py，修一处就全修。

---

## 第三板块：配置与测试文件汇总

### 项目根目录

| 文件 | 类型 | 作用 |
|---|---|---|
| `Makefile` | 构建入口 | 统一任务入口：install / lint / fmt / type / test / pack-lint。通过 `ARGS` 变量支持透传额外参数（如 `make test ARGS="-k roundtrip"`） |
| `IMPLEMENTATION_PLAN.md` | 文档 | 总实施计划，分 P-A/P-B/P-C/P-D/P-E 五个阶段 |
| `P-A_COLLECT_PLAN.md` | 文档 | P-A 采集阶段的详细规格（字段表、哈希定义、不变式等） |
| `.env` | 配置 | 本地环境变量（不进版本库） |
| `.env.example` | 配置 | .env 的模板，告诉新成员需要设哪些变量 |
| `.gitignore` | 配置 | 忽略 `.venv/`、`data/cases/`（产物不入库）等 |
| `.gitattributes` | 配置 | git 行尾属性 |

### 后端项目配置

| 文件 | 类型 | 作用 |
|---|---|---|
| `apps/backend/pyproject.toml` | 构建配置 | 项目名 logcopilot，hatchling 构建，零运行时依赖（domain 层不引框架），dev 依赖只有 pytest 和 ruff。含 ruff / pyright / pytest 的工具配置 |

### 脚本与运行时

| 文件 | 类型 | 作用 |
|---|---|---|
| `scripts/pack_lint.py` | CLI 脚本 | pack-lint 命令入口（第 6 步详解的那个脚本） |
| `runtime/make_sample.py` | 演示脚本 | 造两个样例 pack（正常例 + 故障例），用来 demo `make pack-lint`。**不进版本库** |

### 测试文件

| 文件 | 数量级 | 作用 |
|---|---|---|
| `apps/backend/tests/domain/test_log.py` | ~170 行 | 测 LogRecord 的构造校验、哈希、往返序列化 |
| `apps/backend/tests/domain/test_case.py` | ~390 行 | 测 CaseManifest / Labels / 各子部件的构造校验、from_dict / to_dict 往返 |
| `apps/backend/tests/store/test_casepack.py` | ~520 行 | 测 create_pack / LogWriter / finalize / 哈希 / audit / audit_levels 的完整流程 |
| `apps/backend/tests/collect/test_canonical.py` | ~380 行 | 测 canonical 层的解析逻辑（hdfs_log4j 和 lab 两套 profile） |
| `apps/backend/tests/collect/test_redact.py` | ~260 行 | 测脱敏规则的匹配、幂等、计数留账 |
| `apps/backend/tests/collect/test_adapters_base.py` | ~330 行 | 测 ComponentReader 的连续编号、续采 checkpoint、StreamSpec 校验 |
| `apps/backend/tests/collect/test_adapters_file.py` | ~280 行 | 测 FileReader 的续采三重校验、半行跳过、inode 变更检测 |
| `apps/backend/tests/collect/test_adapters_docker.py` | ~400 行 | 测 DockerReader 的三重校验（用 Mock Runner 不依赖真 Docker）、tail -c 字节偏移对齐 |
| `apps/backend/tests/fixtures/logs/hdfs_log4j.log` | 8 行 | fixture — 标准 Hadoop log4j 格式样例日志 |
| `apps/backend/tests/fixtures/logs/lab_syntax_a.log` | 9 行 | fixture — 实验室风格样例日志 A（旧数据格式） |
| `apps/backend/tests/fixtures/logs/lab_syntax_b.log` | 8 行 | fixture — 实验室风格样例日志 B（旧数据格式） |

### domain / store / collect / adapters 包的 `__init__.py`

| 文件 | 作用 |
|---|---|
| `apps/backend/src/logcopilot/__init__.py` | 顶层包标记 |
| `apps/backend/src/logcopilot/domain/__init__.py` | domain 子包标记 |
| `apps/backend/src/logcopilot/store/__init__.py` | store 子包标记 |
| `apps/backend/src/logcopilot/collect/__init__.py` | collect 子包标记 |
| `apps/backend/src/logcopilot/collect/adapters/__init__.py` | adapters 子包标记，从 base.py 导出 `ComponentReader` / `SourceAdapter` / `StreamSpec` 等 |

（这些文件的作用是让 Python 把目录识别为可 import 的包。`adapters/__init__.py` 额外做了 re-export，外部 `from logcopilot.collect.adapters import ComponentReader` 可以直接拿到关键类。）