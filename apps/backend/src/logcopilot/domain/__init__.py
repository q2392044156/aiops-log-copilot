"""domain 层：纯数据类型与规则，零 IO、零网络、零框架依赖（总计划 §5 硬规则）。

本层被 collect / rag / agents / eval 共同 import，
所以这里出现的任何 import 都会被整条流水线继承 —— 保持 stdlib-only。
"""
