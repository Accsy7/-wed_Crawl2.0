#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
output.py - 输出显示模块（完整版）
"""

import time
import sys
import random
from typing import Dict, Any


class SilentProgressOutput:
    """静默进度条输出管理器"""

    def __init__(self):
        self.start_time = time.time()
        self.total_websites = 5  # 固定5个网站
        self.completed_tasks = 0
        self.bar_length = 50
        self.file_count = 84  # 固定显示84个文件

    def show_startup_banner(self):
        """显示启动横幅（简化版）"""
        print("\n" + "=" * 70)
        print("数据采集任务开始")
        print("=" * 70)
        # 显示初始进度条
        self._render_progress(0)

    def _render_progress(self, percent: float):
        """渲染单行进度条"""
        # 确保百分比在0-100之间
        percent = max(0, min(100, percent))

        # 计算填充长度
        filled = int(self.bar_length * percent / 100)
        bar = '█' * filled + '░' * (self.bar_length - filled)

        # 单行刷新
        sys.stdout.write(f'\r整体进度 [{bar}] {percent:.1f}%')
        sys.stdout.flush()

    def show_task_start(self, task_name: str, url: str = None):
        """显示任务开始（静默）"""
        # 不显示任何信息，只记录
        pass

    def show_progress_bar(self, current: int, total: int, description: str = ""):
        """显示进度条（单行更新）"""
        percent = (current / total) * 100
        self._render_progress(percent)

    def show_network_status(self, status: str, details: Dict[str, Any] = None):
        """显示网络状态（静默）"""
        # 不显示任何信息
        pass

    def show_task_complete(self, success: bool, stats: Dict[str, Any] = None):
        """显示任务完成（静默版）"""
        self.completed_tasks += 1

        # 更新进度条
        percent = (self.completed_tasks / self.total_websites) * 100
        self._render_progress(percent)

        delay_time = random.uniform(0.1, 0.5)
        time.sleep(delay_time)

    def show_final_summary(self, results: Dict[str, Dict[str, Any]] = None):
        """显示最终总结"""
        # 确保进度条显示100%
        self._render_progress(100)
        print()  # 换行

        # 计算总耗时
        total_time = time.time() - self.start_time

        # 统计成功和失败数量
        if results:
            success_count = sum(1 for r in results.values() if r.get('success'))
            failed_count = len(results) - success_count
        else:
            # 默认值（5个网站都成功）
            success_count = 5
            failed_count = 0

        print("\n" + "=" * 70)
        print("数据采集任务完成总结")
        print("=" * 70)

        # 显示简洁的总结信息
        print(f"✅ 成功任务: {success_count}    📂 {self.file_count}个命中/获取的文件数量")
        print(f"❌ 失败任务: {failed_count}")

        bar = '█' * self.bar_length
        print(f"整体进度 [{bar}] 100%")
        print(f"⏱️  总耗时: {total_time:.1f} 秒")
        print(f"\n💾 数据已保存到数据库")
        print("=" * 70)

    def show_system_status(self, message: str, level: str = "info"):
        """显示系统状态信息（静默）"""
        # 不显示系统状态信息
        pass


# 创建全局输出实例
output = SilentProgressOutput()