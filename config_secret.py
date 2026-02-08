#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config_secret.py - 敏感配置模块（不可展示）
包含本地文件路径、数据库连接等敏感信息
"""

import os
from pathlib import Path


def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent


def get_sensitive_config():
    """
    获取敏感配置
    
    Returns:
        敏感配置字典
    """
    project_root = get_project_root()
    
    # ========================================================
    # 以下为敏感配置，请根据实际情况修改，但不要展示给他人
    # ========================================================
    
    sensitive_config = {
        "广西政府网": {
            "local_cache_path": str(project_root / "data" / "广西政府网" / "government_data.xlsx"),
            "data_folder": str(project_root / "data" / "广西政府网"),
            "db_table_prefix": "gx_gov"
        },
        "广西自然资源厅": {
            "local_cache_path": str(project_root / "data" / "自然资源厅" / "land_resources.xlsx"),
            "data_folder": str(project_root / "data" / "自然资源厅"),
            "db_table_prefix": "gx_dnr"
        },
        "三资财务管理平台": {
            "local_cache_path": str(project_root / "data" / "三资平台" / "financial_data.xlsx"),
            "data_folder": str(project_root / "data" / "三资平台"),
            "db_table_prefix": "acct_finance"
        },
        "广西统计局": {
            "local_cache_path": str(project_root / "data" / "统计局" / "statistics_data.xlsx"),
            "data_folder": str(project_root / "data" / "统计局"),
            "db_table_prefix": "gx_stats"
        },
        "广西税务局": {
            "local_cache_path": str(project_root / "data" / "税务局" / "tax_data.xlsx"),
            "data_folder": str(project_root / "data" / "税务局"),
            "db_table_prefix": "gx_tax"
        }
    }
    
    # 数据库配置
    database_config = {
        "database_path": str(project_root / "data" / "crawled_data.db"),
        "database_type": "sqlite",
        "connection_timeout": 30,
        "pool_size": 5
    }
    
    # 网络配置
    network_config = {
        "proxy_enabled": False,
        "proxy_url": "http://proxy.example.com:8080",
        "timeout": 30,
        "retry_count": 3
    }
    
    # 将所有配置合并
    full_config = {
        "sensitive": sensitive_config,
        "database": database_config,
        "network": network_config
    }
    
    return full_config


def get_database_config():
    """获取数据库配置"""
    config = get_sensitive_config()
    return config.get("database", {})


def get_website_sensitive_config(website_name):
    """获取指定网站的敏感配置"""
    config = get_sensitive_config()
    sensitive = config.get("sensitive", {})
    return sensitive.get(website_name, {})


def ensure_data_directories():
    """确保数据目录存在"""
    config = get_sensitive_config()
    sensitive = config.get("sensitive", {})
    
    print("📁 创建数据目录...")
    for website, website_config in sensitive.items():
        data_folder = website_config.get("data_folder")
        if data_folder:
            os.makedirs(data_folder, exist_ok=True)
            print(f"   ✅ {website}: {data_folder}")
    
    # 创建数据库目录
    db_config = config.get("database", {})
    db_path = db_config.get("database_path")
    if db_path:
        db_dir = os.path.dirname(db_path)
        os.makedirs(db_dir, exist_ok=True)
        print(f"   ✅ 数据库目录: {db_dir}")
    
    print("✅ 目录结构就绪")


if __name__ == "__main__":
    # 测试配置加载
    config = get_sensitive_config()
    print("配置加载成功")
    print(f"数据库路径: {config.get('database', {}).get('database_path')}")
    
    # 创建目录
    ensure_data_directories()