# 顺序 1 · 翻译的第三步：脱敏（与 canonical.py 同属节点 1，必须排在它后面）
"""redact —— A2 的第三步：脱敏（P-A §6.2 ③、V8）。

三条必须守住的规则：

  1. **幂等**。旧 132 例的日志早就被脱敏过一遍了，满屏是 `<:IP:>`、`<:PATH:>`、
     `<:BLOCK:>` 这类现成占位符。再脱一遍如果改动其中任何一个字符，
     `canonical_sha256` 就会变 —— V4（file 与 docker 两条路径哈希相等）
     和"两哈希不变式"当场全废。
     做法：先把已有占位符换成哨兵 → 跑规则 → 把哨兵还原回占位符。

  2. **计数要留账**（V8）。每条规则命中几次都返回给调用方，
     最终落进 `manifest.counters.redacted`。脱敏是个"看不见的过滤器"，
     没有计数就没人能发现规则失效了。

  3. **宁可少脱，不可错脱**。规则写得越宽，越容易把 `org.apache.hadoop...`
     这种关键信息当成域名脱掉，诊断时再也认不出是哪个类报的错。
     所以每条规则都取窄：IP 要完整四段、域名要有 TLD、路径要以分隔符开头。

规则闭集与占位符形态与 `domain/case.py` 的 `REDACT_RULES`、
旧数据里已经在用的 `<:IP:>` 风格保持一致。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from logcopilot.domain.case import REDACT_RULES

__all__ = ["DEFAULT_ORDER", "PLACEHOLDERS", "REDACT_VERSION", "PatternRedactor"]

#: 脱敏策略版本，写进 `manifest.redact_policy.version`。
#: 它**不进**任何一个哈希（P-A §6.4 的排除项）—— 规则升级不该让历史 pack 的
#: 指纹变脏，否则你永远说不清"这条记录变没变"到底是数据变了还是规则变了。
REDACT_VERSION: Final[str] = "redact/1.0.0"

#: 四类规则各对应一个占位符。大写的 `<:NAME:>` 形态是旧数据一直在用的，
#: 沿用它是为了让新旧 pack 的文本观感一致（也方便人肉 grep）。
PLACEHOLDERS: Final[dict[str, str]] = {
    "host": "<:HOST:>",
    "ip": "<:IP:>",
    "path": "<:PATH:>",
    "token": "<:TOKEN:>",
}

#: 规则执行顺序。谁先命中就归谁计数，所以顺序是有语义的：
#:   ip    —— 最具体，先抓。`10.0.0.1` 不该先被 path 或 token 吃掉
#:   token —— 密钥值里常含路径或 IP，先抓才能把整段算进 token
#:   host  —— 主机名形态宽松，放在具体规则之后
#:   path  —— 最能"吞"东西的一条，最后跑
DEFAULT_ORDER: Final[tuple[str, ...]] = ("ip", "token", "host", "path")

#: 已存在的占位符：`<:IP:>`、`<:PATH:>`、`<:BLOCK:>`……
#: 允许任意大写标识符，是因为旧数据里还出现过不在我们闭集里的占位符（如 BLOCK）。
_PLACEHOLDER_RE: Final[re.Pattern[str]] = re.compile(r"<:[A-Z][A-Z0-9_]*:>")

#: 保护占位符用的哨兵。选 `\x00` 是因为文本日志里不可能出现 NUL 字节。
_SENTINEL_PREFIX: Final[str] = "\x00ph"

# ---------------------------------------------------------------- 各规则


#: IPv4 的四段数字。逐段限制 0–255，避免把 `3.3.6.0` 之外的版本号误判成 IP
#: （`3.3.6` 只有三段，本来也不会命中，但四段版本号是真实存在的写法）。
_OCTET: Final[str] = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"

#: 前后都不许贴着字母、点或横线：`BP-466928225-<:IP:>-1793....` 这种拼接串里
#: 只能抓到真正独立的那一段。
_IP: Final[re.Pattern[str]] = re.compile(rf"(?<![\w.-]){_OCTET}(?:\.{_OCTET}){{3}}(?![\w.-])")

#: 主机名。三条路都要走，因为三种真实形态完全不同：
#:   `localhost`                        容器内部互访
#:   `WIN-20260727MVX`                  旧实验台的 Windows 机器名
#:   `hdfs-datanode2`                   容器名（也是 bind mount 的目录名）
#:   `foo.corp.local`                   带 TLD 的域名
#:
#: 故意不匹配的三类：
#:   `datanode2` / `DataNode`           Hadoop 的组件名与类名，脱掉等于删日志
#:   `aiops-lab-dn1`                    旧数据的 `[component] ` 前缀，是**结构标签**
#:                                      不是主机名（canonicalize 已经把它归一成 dn1），
#:                                      把它脱成 `<:HOST:>` 会让 raw 里的前缀消失、
#:                                      计数虚增，还会让新旧 pack 的 raw 不可比
#:   `a.b.c` 这种短点分词            没有 TLD 就不像域名
_HOST: Final[re.Pattern[str]] = re.compile(
    r"\b(?:"
    r"localhost"
    r"|WIN-[A-Z0-9]{6,}"
    r"|hdfs-(?:nn|namenode|dn[1-9]|datanode[1-9])[A-Za-z0-9-]*"
    r"|[A-Za-z0-9][A-Za-z0-9-]*(?:\.[A-Za-z0-9-]+)*\.(?:local|internal|lan|corp|home|example)"
    r")\b"
)

#: 绝对路径。以分隔符开头（`/`、`C:\`）是硬条件 —— 只有绝对路径才带
#: "这台机器的目录结构"这种隐私。
#:
#: 开头那段零宽断言是两选一：
#:   `(?<=file:)`            让 `file:/data/1` 里的 `/data/1` 也能被抓住
#:   `(?<![\w<:./\\])`       其余情况下前面不能贴着字母、`<`、`:`、点、斜杠或反斜杠
#: 后半句同时挡掉三种误伤：`and/or` 这种词内斜杠、`/<:IP:>` 这种已脱敏占位符、
#: 以及 `-1793205428863/current/finalized` 这种 id 后面的相对片段。
_PATH: Final[re.Pattern[str]] = re.compile(
    r"(?:(?<=file:)|(?<![\w<:./\\]))"
    r"(?:[A-Za-z]:[\\/]|/)"
    r"(?:[\w.@+~-]+[\\/])*"
    r"[\w.@+~-]+"
)

#: `key=value` 形态的密钥。这条带捕获组：要把 `password=` 这个键名留下，
#: 只替换值 —— 否则读日志的人连"这是密码字段"都看不出来。
#:
#: 值不能含 `\x00`：占位符在规则跑之前已被换成哨兵（`\x00ph0\x00`），
#: `password=<:TOKEN:>` 这种"已经脱过的"必须认不出来，否则每跑一次
#: 计数就涨一次 —— 文本没变、账却在虚增，V8 的"脱敏有账"就成了假账。
_SECRET_KV: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(token|password|passwd|secret|api[_-]?key)\b(\s*[=:]\s*)([^\s\x00]+)"
)

#: 独立形态的密钥：云厂商 key、GitHub token、JWT。
_TOKEN: Final[re.Pattern[str]] = re.compile(
    r"(?:sk-[A-Za-z0-9]{16,}"
    r"|ghp_[A-Za-z0-9]{20,}"
    r"|AKIA[0-9A-Z]{16}"
    r"|eyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]{8,})"
)


@dataclass(frozen=True, slots=True)
class _Rule:
    """一条脱敏规则。

    `keep_groups` 是"替换时保留前几个捕获组" —— 密钥那条要保留键名与等号，
    其余三条整段替换。

    `sentinel` 是这条规则替换时**先写进去的东西**，不是最终占位符。
    这样做是因为：如果直接写 `<:TOKEN:>`，后面跑的 `password=…` 那条规则会把
    刚写进去的 `<:TOKEN:>` 当成密钥值再脱一遍，文本变成
    `<:TOKEN:<:TOKEN:>`（真踩过）。先写哨兵、最后统一换占位符，
    任何一条规则的输出都不会被下一条规则再吃到。
    """

    name: str
    pattern: re.Pattern[str]
    placeholder: str
    sentinel: str
    keep_groups: int = 0

    def _render(self, match: re.Match[str]) -> str:
        kept = "".join(match.group(i) for i in range(1, self.keep_groups + 1))
        return kept + self.sentinel

    def apply(self, text: str) -> tuple[str, int]:
        count = 0

        def repl(match: re.Match[str]) -> str:
            nonlocal count
            count += 1
            return self._render(match)

        return self.pattern.sub(repl, text), count


def _build_rules(order: tuple[str, ...]) -> tuple[_Rule, ...]:
    """按 ``order`` 组装规则。四种形态里只有密钥那条要保留键名。"""
    builders: dict[str, tuple[re.Pattern[str], int]] = {
        "ip": (_IP, 0),
        "host": (_HOST, 0),
        "path": (_PATH, 0),
    }
    rules: list[_Rule] = []
    for name in order:
        if name == "token":
            # 两种 token 写法：先整段（sk-/ghp_…），再 `key=value`（保留键名）。
            shapes: list[tuple[re.Pattern[str], int]] = [(_TOKEN, 0), (_SECRET_KV, 2)]
        else:
            shapes = [builders[name]]
        for pattern, keep_groups in shapes:
            index = len(rules)
            rules.append(
                _Rule(
                    name=name,
                    pattern=pattern,
                    placeholder=PLACEHOLDERS[name],
                    sentinel=f"{_SENTINEL_PREFIX}rd{index}\x00",
                    keep_groups=keep_groups,
                )
            )
    return tuple(rules)


class PatternRedactor:
    """四类规则的正则脱敏器，满足 `canonical.Redactor` 协议。

    用法（第 7 步的 `collect.py` 会这么接）：:

        result = canonicalize(lines, ..., redact=PatternRedactor())

    命中计数按**规则名**累加，多个子规则同属 `token` 时合并成一个计数 ——
    `counters.redacted` 的键必须是 `REDACT_RULES` 闭集里的名字。
    """

    version: str = REDACT_VERSION

    def __init__(self, rules: tuple[str, ...] | list[str] = DEFAULT_ORDER) -> None:
        unknown = [name for name in rules if name not in REDACT_RULES]
        if unknown:
            raise ValueError(f"未知脱敏规则 {unknown}，闭集={list(REDACT_RULES)}")
        if not rules:
            raise ValueError("rules 不能为空 —— 空规则等于声明'已脱敏'但什么都没做")
        self._order: tuple[str, ...] = tuple(rules)
        self._rules: tuple[_Rule, ...] = _build_rules(self._order)

    @property
    def rules(self) -> tuple[str, ...]:
        """实际启用的规则名，写进 `manifest.redact_policy.rules`。"""
        return self._order

    def apply(self, text: str) -> tuple[str, dict[str, int]]:
        """脱敏一段文本，返回 `(脱敏后文本, {规则名: 命中次数})`。

        幂等靠两步：
          1. 先把**已有**占位符换成哨兵 —— 否则 `<:IP:>` 会被 IP 规则当成
             "又一个可疑字符串"再处理一遍，第二次跑出来就和第一次不一样。
          2. 规则替换时写哨兵、**全部跑完**才统一换成占位符 —— 否则前一条规则
             写进去的 `<:TOKEN:>` 会被后一条 `password=` 规则当成密钥值再吃一遍。
        """
        if not text:
            return text, {}

        saved: dict[str, str] = {}

        def protect(match: re.Match[str]) -> str:
            token = f"{_SENTINEL_PREFIX}{len(saved)}\x00"
            saved[token] = match.group(0)
            return token

        work = _PLACEHOLDER_RE.sub(protect, text)

        counts: dict[str, int] = {}
        for rule in self._rules:
            work, hits = rule.apply(work)
            if hits:
                counts[rule.name] = counts.get(rule.name, 0) + hits

        for token, placeholder in saved.items():
            work = work.replace(token, placeholder)
        for rule in self._rules:
            work = work.replace(rule.sentinel, rule.placeholder)

        return work, counts
