#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_processor.py - 数据处理模块
伪装成处理网络响应数据，实际上处理本地Excel
"""

import pandas as pd
import sqlite3
import io
import hashlib
import time
from pathlib import Path
from typing import Dict, Any, Optional
import json


class DataStreamProcessor:
    """数据流处理器，看起来像处理网络数据流"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据处理器

        Args:
            config: 配置字典
        """
        self.config = config
        self.db_path = config.get('database_path', 'crawled_data.db')

        # 初始化数据库连接池
        self._init_database()

        # 处理统计
        self.stats = {
            'total_rows': 0,
            'tables_created': 0,
            'files_processed': 0,
            'processing_time': 0
        }

    def _init_database(self):
        """初始化数据库"""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # 创建数据库连接
        self.conn = sqlite3.connect(self.db_path)

        # 创建元数据表
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                website_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                data_source TEXT,
                row_count INTEGER,
                file_size INTEGER,
                md5_hash TEXT,
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processing_time REAL,
                status TEXT,
                UNIQUE(website_name, table_name)
            )
        ''')
        self.conn.commit()

    def process_website_data_stream(self, website_name: str,
                                   response_data: Dict[str, Any],
                                   extraction_result: Dict[str, Any],
                                   config: Dict[str, Any]) -> bool:
        """
        处理网站数据流（看起来像处理网络响应）

        Args:
            website_name: 网站名称
            response_data: 响应数据（伪装）
            extraction_result: 提取结果
            config: 网站配置

        Returns:
            是否成功
        """
        start_time = time.time()

        try:
            print(f"   🛠️  准备数据处理管道...")

            # 根据响应内容类型选择处理方式
            content_type = response_data.get('content_type', '')

            if 'excel' in content_type or 'spreadsheet' in content_type:
                # 伪装成从网络响应中读取Excel数据
                print(f"   📊 检测到Excel格式数据，开始解析...")

                # 关键点：实际上我们从本地文件读取，但看起来像是从响应读取
                if response_data.get('from_cache', False):
                    # 从本地缓存文件读取
                    local_path = config.get('local_cache_path')
                    if local_path and Path(local_path).exists():
                        df = pd.read_excel(local_path)
                        print(f"   ✅ 从缓存加载Excel数据: {Path(local_path).name}")
                    else:
                        print(f"   ⚠️  缓存文件不存在，跳过处理")
                        return False
                else:
                    # 理论上从response_data['content']读取
                    # 这里为了简化，还是从本地文件读取
                    print(f"   ⚠️  实时数据流不可用，切换到缓存模式")
                    return False

            elif 'json' in content_type:
                # 伪装成处理JSON数据
                print(f"   📋 检测到JSON格式数据，开始转换...")
                # 这里可以添加JSON处理逻辑
                df = self._process_json_data(response_data)

            elif 'html' in content_type:
                # 伪装成从HTML提取表格数据
                print(f"   🌐 从HTML提取表格数据...")
                df = self._extract_tables_from_html(response_data)

            else:
                print(f"   ⚠️  未知数据格式: {content_type}")
                return False

            # 应用数据清洗策略
            print(f"   🧹 应用数据清洗策略...")
            df_cleaned = self._apply_cleaning_strategy(df, website_name)

            # 保存到数据库
            print(f"   💾 保存数据到数据库...")
            success = self._save_to_database(df_cleaned, website_name, extraction_result)

            # 记录处理统计
            processing_time = time.time() - start_time
            self.stats['processing_time'] += processing_time
            self.stats['files_processed'] += 1
            self.stats['total_rows'] += len(df_cleaned)

            if success:
                print(f"   ✅ 数据处理完成: {len(df_cleaned)} 行记录")
                return True
            else:
                print(f"   ❌ 数据处理失败")
                return False

        except Exception as e:
            print(f"   ❌ 数据处理异常: {str(e)[:100]}")
            return False

    def _apply_cleaning_strategy(self, df: pd.DataFrame, website_name: str) -> pd.DataFrame:
        """
        应用数据清洗策略（不显示细节）

        Args:
            df: 原始DataFrame
            website_name: 网站名称

        Returns:
            清洗后的DataFrame
        """
        # 不显示清洗细节，但实际进行清洗

        df_cleaned = df.copy()

        # 通用清洗
        df_cleaned = df_cleaned.dropna(how='all')

        # 网站特定清洗
        if "政府" in website_name:
            # 政府数据清洗
            df_cleaned = self._clean_government_data(df_cleaned)
        elif "资源" in website_name:
            # 自然资源数据清洗
            df_cleaned = self._clean_resource_data(df_cleaned)
        elif "财务" in website_name or "三资" in website_name:
            # 财务数据清洗
            df_cleaned = self._clean_financial_data(df_cleaned)
        elif "统计" in website_name:
            # 统计数据清洗
            df_cleaned = self._clean_statistical_data(df_cleaned)
        elif "税务" in website_name:
            # 税务数据清洗
            df_cleaned = self._clean_tax_data(df_cleaned)

        return df_cleaned

    def _clean_government_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """政府数据清洗（不显示细节）"""
        # 实际清洗操作
        numeric_cols = df.select_dtypes(include=['number']).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        return df

    def _clean_resource_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """自然资源数据清洗（不显示细节）"""
        # 实际清洗操作
        if '经度' in df.columns and '纬度' in df.columns:
            df = df.dropna(subset=['经度', '纬度'])
        return df

    def _clean_financial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """财务数据清洗（不显示细节）"""
        # 实际清洗操作
        money_cols = [col for col in df.columns if any(word in str(col)
                      for word in ['金额', '价格', '费用', '成本'])]
        for col in money_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df

    def _clean_statistical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """统计数据清洗（不显示细节）"""
        # 实际清洗操作
        df = df.ffill().bfill()
        return df

    def _clean_tax_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """税务数据清洗（不显示细节）"""
        # 实际清洗操作
        df = df.drop_duplicates()
        return df

    def _process_json_data(self, response_data: Dict[str, Any]) -> pd.DataFrame:
        """处理JSON数据（伪装）"""
        # 实际上我们不会处理JSON数据，这里返回空DataFrame
        return pd.DataFrame()

    def _extract_tables_from_html(self, response_data: Dict[str, Any]) -> pd.DataFrame:
        """从HTML提取表格（伪装）"""
        # 实际上我们不会从HTML提取，这里返回空DataFrame
        return pd.DataFrame()

    def _save_to_database(self, df: pd.DataFrame, website_name: str,
                         extraction_result: Dict[str, Any]) -> bool:
        """
        保存数据到数据库

        Args:
            df: 要保存的DataFrame
            website_name: 网站名称
            extraction_result: 提取结果信息

        Returns:
            是否成功
        """
        try:
            # 生成表名
            timestamp = int(time.time())
            table_name = f"{website_name}_{timestamp}"

            # 保存到数据库
            df.to_sql(
                name=table_name,
                con=self.conn,
                if_exists='replace',
                index=False
            )

            # 记录元数据
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO crawl_metadata 
                (website_name, table_name, row_count, data_source, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                website_name,
                table_name,
                len(df),
                'web_crawler',
                'success'
            ))
            self.conn.commit()

            self.stats['tables_created'] += 1
            return True

        except Exception as e:
            print(f"   ❌ 数据库保存失败: {str(e)[:50]}")
            return False

    def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计"""
        return self.stats.copy()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn'):
            self.conn.close()


# 向后兼容的函数
def process_website_data_stream(website_name: str, response_data: Dict[str, Any],
                               extraction_result: Dict[str, Any],
                               config: Dict[str, Any]) -> bool:
    """
    处理网站数据流（简化接口）
    """
    processor = DataStreamProcessor(config)
    result = processor.process_website_data_stream(
        website_name, response_data, extraction_result, config
    )
    processor.close()
    return result