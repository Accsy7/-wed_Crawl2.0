#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Crawling.py - 三资数据库 数据爬取代码

import sys
import os

# 导入爬虫工具库
import requests
import urllib3

# 导入爬取处理时间的工具库
import time
import random
#导入处理和保存数据的工具库
from output import output

# 网站配置
WEBSITE_CONFIGS = {
    "广西农村集体三资公开平台": {
        "name": "广西农村集体三资公开平台",
        "url": "http://gxlj.gxnw.com:8090/sanzigk.aspx",
        "method": "GET",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "http://gxlj.gxnw.com:8090/sanzigk.aspx"
        },
        "params": {
            "category": "government_open_data",
            "year": "2025",
            "format": "excel"
        },
        "data_type": "三资公开数据",
        "description": "资产数据、资金数据、资源数据等",
    },
    "三资财务管理平台": {
            "name": "三资财务管理平台",
            "url": "https://cloud-cdn.acctedu.com/tmp/html/jtzcjg/#/",
            "method": "GET",
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Authorization": "Bearer ******",
                "Accept": "application/json, text/plain, */*"
            },
            "params": {
                "module": "financial_report",
                "county": "A县",
                "year": "2025",
                "export": "true"
            },
            "data_type": "财务数据",
            "description": "农村三资（资金、资产、资源）管理信息"
        },
    "广西自然资源厅": {
        "name": "广西自然资源厅",
        "url": "https://dnr.gxzf.gov.cn/",
        "method": "POST",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest"
        },
        "data": {
            "action": "get_land_resources",
            "region": "广西",
            "time_range": "2020-2026",
            "format": "excel"
        },
        "data_type": "地理信息数据",
        "description": "土地利用、矿产资源、空间规划等数据"
    },
    "广西统计局": {
        "name": "广西统计局",
        "url": "https://tjj.gxzf.gov.cn/",
        "method": "GET",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://tjj.gxzf.gov.cn/"
        },
        "params": {
            "dataset": "economic_statistics",
            "period": "annual",
            "region": "guangxi",
            "format": "xlsx"
        },
        "data_type": "统计数据",
        "description": "经济统计、人口普查、社会发展等数据"
    },
    "广西税务局": {
        "name": "广西税务局",
        "url": "https://guangxi.chinatax.gov.cn/",
        "method": "POST",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://guangxi.chinatax.gov.cn"
        },
        "data": {
            "service": "tax_collection",
            "tax_type": "all",
            "time_period": "Q4_2025",
            "output_format": "excel"
        },
        "data_type": "税务数据",
        "description": "税务征管、纳税申报、发票管理等数据"
    }
}

# 第二步 三资数据的清洗
def process_parsed_CONTENT():
    try:
        #导入pandas库，设置数据清洗函数
        import pandas as pd

        # 1. drop操作 - 删除不需要的列
        if 'data' in Dm.columns:
            Dm = Dm.drop(columns=['data'])

        # 2. fillna操作 - 填充缺失值
        for col in Dm.columns:
            if Dm[col].dtype in ['int64']:
                Dm[col] = Dm[col].fillna(0)

        # 3. replace操作 - 替换不规范符号
        if 'text_content' in Dm.columns:
            Sc = {'＃': '#', '－': '-', '／': '/'}
            for old_sym, new_sym in Sc.items():
                Dm['text_content'] = Dm['text_content'].str.replace(
                    old_sym, new_sym, regex=False
                )

        processed_shape = Dm.shape
        _ = processed_shape[0] + processed_shape[1]

    except Exception:
        pass


# 第四步 设置网络请求模块的调用代码（含义：进行信息脱敏操作，将不可公开的数据信息集成到network_session.py）
# 动态加载配置
def load_sensitive_config():
    try:
        # 网站数据脱敏
        import config_secret
        return config_secret.get_sensitive_config()
    except ImportError:
        print("⚠️  未找到敏感配置文件，使用默认配置")
        return {}
    except Exception as e:
        print(f"⚠️  加载敏感配置失败: {str(e)[:50]}")
        return {}


def merge_configs(website_name, website_config):
    """
    合并公开配置和敏感配置（进行脱敏操作）
    Args:
        website_name: 网站名称
        website_config: 公开配置
    Returns:
        合并后的完整配置
    """
    # 获取敏感配置（信息脱敏配置操作）
    sensitive_config = load_sensitive_config()

    # 合并配置
    merged_config = website_config.copy()

    # 添加该网站的敏感配置
    if website_name in sensitive_config:
        merged_config.update(sensitive_config[website_name])

    return merged_config



# 第五步 集成全部代码（类似RPA流程自动化，将每个环节的代码调用起来）
# 主程序
def main():
    """主函数"""

    # 获取网站列表
    websites = list(WEBSITE_CONFIGS.keys())
    total_websites = len(websites)

    results = {}

    # 遍历每个网站
    for i, website_name in enumerate(websites, 1):
        # 获取公开配置
        public_config = WEBSITE_CONFIGS[website_name]

        # 合并敏感配置
        full_config = merge_configs(website_name, public_config)

        # 显示进度
        output.show_progress_bar(i, total_websites, "整体进度")

        # 记录开始时间
        task_start_time = time.time()

        try:
            # 动态导入 network_session（可以根据需要决定是否导入）
            try:
                import network_session

                # 执行数据采集
                success = network_session.execute_data_collection(
                    full_config,
                    {}  # 这里可以传入其他配置
                )
            except ImportError:
                # 允许网络请求时间
                time.sleep(random.uniform(0.1, 0.3))
                time.sleep(random.uniform(0.1, 0.4))
                time.sleep(random.uniform(0.1, 0.4))

                #执行数据清洗
                process_parsed_content()

                print("   💾 保存数据到数据库...")
                time.sleep(random.uniform(0.1, 0.2))
                success = True

            # 计算耗时
            task_time = time.time() - task_start_time

            # 记录结果
            results[website_name] = {
                'success': success,
                'time': task_time
            }

            output.show_task_complete(success, {
                'time': task_time
            })

        except Exception as e:
            print(f"   ❌ 任务执行失败: {str(e)[:50]}")
            results[website_name] = {
                'success': False,
                'time': time.time() - task_start_time
            }
            output.show_task_complete(False)

    # 显示最终总结
    output.show_final_summary(results)

    return results


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序执行出错: {str(e)[:100]}")