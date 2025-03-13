"""
报告生成模块，负责生成质量分析报告
"""
import logging
import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markdownify import markdownify as md
import base64
from io import BytesIO
import matplotlib.font_manager as fm
import platform

# Try to set a font that supports Chinese characters
def setup_font():
    system = platform.system()
    if system == 'Windows':
        # Common fonts on Windows with CJK support
        font_candidates = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
    elif system == 'Darwin':  # macOS
        # Common fonts on macOS with CJK support
        font_candidates = ['PingFang SC', 'Heiti SC', 'STHeiti', 'Arial Unicode MS']
    else:  # Linux and others
        # Common fonts on Linux with CJK support
        font_candidates = ['Noto Sans CJK SC', 'WenQuanYi Micro Hei', 'Droid Sans Fallback']
    
    # Try each font until we find one that exists
    for font in font_candidates:
        try:
            if any(f for f in fm.findSystemFonts() if font.lower() in f.lower()):
                plt.rcParams['font.family'] = font
                return True
        except:
            continue
    
    return False

# If no CJK font is found, use English labels
if not setup_font():
    # Set a safe default font
    plt.rcParams['font.family'] = 'sans-serif'

class ReportGenerator:
    """报告生成器，负责创建质量分析报告"""
    
    def __init__(self, config, gemini_integrator=None):
        """
        初始化报告生成器
        
        Args:
            config (Config): 配置对象
            gemini_integrator (GeminiIntegrator, optional): Gemini集成器
        """
        self.config = config
        self.gemini = gemini_integrator
        self.output_format = config.get('output.format', 'html')
        self.output_path = config.get('output.path', './reports')
        self.output_filename = config.get('output.filename', 'pgsql_quality_report')
        self.show_plots = config.get('output.show_plots', True)
        
        # 创建输出目录
        os.makedirs(self.output_path, exist_ok=True)
        
        # 初始化Jinja2环境
        template_path = os.path.join(os.path.dirname(__file__), 'templates')
        if not os.path.exists(template_path):
            template_path = os.path.join(os.getcwd(), 'templates')
        
        # Always create default templates
        os.makedirs(template_path, exist_ok=True)
        self._create_default_templates(template_path)
        
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_path),
            autoescape=select_autoescape(['html', 'xml'])
        )
    
    def generate_report(self, analysis_results):
        """
        生成质量分析报告
        
        Args:
            analysis_results (dict): 分析结果
            
        Returns:
            str: 报告文件路径
        """
        # 提取分析结果
        schema_info = analysis_results.get('schema_info', {})
        tables_info = analysis_results.get('tables_info', {})
        quality_results = analysis_results.get('quality_results', {})
        optimization_results = analysis_results.get('optimization_results', {})
        
        # 创建报告上下文
        context = self._create_report_context(
            schema_info, tables_info, quality_results, optimization_results
        )
        
        # 如果启用了Gemini，获取AI见解
        if self.gemini and self.config.get('gemini.enable'):
            try:
                # 创建简化的摘要数据
                summary_data = self._create_summary_data(context)
                
                # 获取AI见解
                ai_insights = self.gemini.generate_report_insights(summary_data)
                context['ai_insights'] = ai_insights
            except Exception as e:
                logging.error(f"获取AI报告见解失败: {str(e)}")
                context['ai_insights'] = {"error": str(e)}
        
        # 生成报告
        if self.output_format == 'html':
            return self._generate_html_report(context)
        elif self.output_format == 'markdown':
            return self._generate_markdown_report(context)
        elif self.output_format == 'json':
            return self._generate_json_report(context)
        else:
            logging.warning(f"不支持的输出格式: {self.output_format}，使用HTML格式")
            return self._generate_html_report(context)
    
    def _create_report_context(self, schema_info, tables_info, quality_results, optimization_results):
        """
        创建报告上下文
        
        Args:
            schema_info (dict): Schema信息
            tables_info (dict): 表信息
            quality_results (dict): 质量检查结果
            optimization_results (dict): 优化建议
            
        Returns:
            dict: 报告上下文
        """
        # 创建基本上下文
        context = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'schema_name': schema_info.get('name', 'unknown'),
            'db_name': self.config.get('database.dbname', 'unknown'),
            'table_count': len(tables_info),
            'schema_info': schema_info,
            'tables_info': tables_info,
            'quality_results': quality_results,
            'optimization_results': optimization_results,
            'report_sections': []
        }
        
        # 添加报告部分
        context['report_sections'] = [
            {
                'id': 'overview',
                'title': '概述',
                'content': self._create_overview_section(schema_info, tables_info, quality_results)
            },
            {
                'id': 'schema_analysis',
                'title': 'Schema分析',
                'content': self._create_schema_section(schema_info, tables_info)
            },
            {
                'id': 'data_quality',
                'title': '数据质量分析',
                'content': self._create_quality_section(tables_info, quality_results)
            },
            {
                'id': 'optimization',
                'title': '优化建议',
                'content': self._create_optimization_section(optimization_results)
            }
        ]
        
        # 创建表摘要
        context['table_summaries'] = self._create_table_summaries(tables_info, quality_results)
        
        # 创建数据质量摘要
        context['quality_summary'] = self._create_quality_summary(quality_results)
        
        return context
    
    def _create_overview_section(self, schema_info, tables_info, quality_results):
        """
        创建概述部分
        
        Args:
            schema_info (dict): Schema信息
            tables_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 概述部分内容
        """
        # 计算总体统计
        total_rows = sum(
            table.get('info', {}).get('approximate_row_count', 0) 
            for table in tables_info.values()
        )
        
        total_columns = sum(
            table.get('column_count', 0) for table in tables_info.values()
        )
        
        # 计算质量问题
        total_issues = 0
        issue_by_type = {}
        
        for table_name, table_results in quality_results.items():
            for checker_type, results in table_results.items():
                if isinstance(results, dict) and 'issues' in results:
                    issues = results.get('issues', [])
                    total_issues += len(issues)
                    
                    # 按类型统计问题
                    for issue in issues:
                        issue_type = issue.get('type', 'unknown')
                        if issue_type not in issue_by_type:
                            issue_by_type[issue_type] = 0
                        issue_by_type[issue_type] += 1
        
        # 创建图表（如果启用）
        charts = {}
        if self.show_plots:
            # 表大小分布图
            charts['table_size_chart'] = self._create_table_size_chart(tables_info)
            
            # 问题类型分布图
            if issue_by_type:
                charts['issue_type_chart'] = self._create_issue_type_chart(issue_by_type)
        
        return {
            'summary_stats': {
                'schema_name': schema_info.get('name', 'unknown'),
                'table_count': len(tables_info),
                'total_rows': total_rows,
                'total_columns': total_columns,
                'total_issues': total_issues,
                'relationship_count': schema_info.get('relationship_count', 0)
            },
            'issue_summary': {
                'total_issues': total_issues,
                'issue_by_type': issue_by_type
            },
            'charts': charts
        }
    
    def _create_schema_section(self, schema_info, tables_info):
        """
        创建Schema分析部分
        
        Args:
            schema_info (dict): Schema信息
            tables_info (dict): 表信息
            
        Returns:
            dict: Schema分析部分内容
        """
        # 表大小分析
        table_sizes = []
        for table_name, table_info in tables_info.items():
            table_sizes.append({
                'name': table_name,
                'row_count': table_info.get('info', {}).get('approximate_row_count', 0),
                'total_size': table_info.get('info', {}).get('total_bytes', 0),
                'column_count': table_info.get('column_count', 0)
            })
        
        # 按行数排序
        table_sizes = sorted(table_sizes, key=lambda x: x['row_count'], reverse=True)
        
        # 关系分析
        relationships = schema_info.get('relationships', {})
        rel_analysis = {
            'total_relationships': sum(len(rels) for rels in relationships.values()),
            'tables_with_relationships': len([t for t in relationships if relationships[t]]),
            'relationship_details': relationships
        }
        
        # 列类型分析
        column_types = {}
        for table_info in tables_info.values():
            for column in table_info.get('columns', []):
                data_type = column.get('data_type', 'unknown')
                if data_type not in column_types:
                    column_types[data_type] = 0
                column_types[data_type] += 1
        
        # 创建图表（如果启用）
        charts = {}
        if self.show_plots:
            # 列数据类型分布图
            if column_types:
                charts['column_type_chart'] = self._create_column_type_chart(column_types)
            
            # 关系图（如果表数量不太多）
            if len(tables_info) <= 20:
                charts['relationship_chart'] = self._create_relationship_chart(relationships, tables_info)
        
        return {
            'table_sizes': table_sizes,
            'relationships': rel_analysis,
            'column_types': column_types,
            'charts': charts
        }
    
    def _create_quality_section(self, tables_info, quality_results):
        """
        创建数据质量分析部分
        
        Args:
            tables_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 数据质量分析部分内容
        """
        # 按检查器类型汇总问题
        issues_by_checker = {}
        total_issues_by_table = {}
        
        for table_name, table_results in quality_results.items():
            total_issues_by_table[table_name] = 0
            
            for checker_type, results in table_results.items():
                if isinstance(results, dict) and 'issues' in results:
                    issues = results.get('issues', [])
                    total_issues_by_table[table_name] += len(issues)
                    
                    if checker_type not in issues_by_checker:
                        issues_by_checker[checker_type] = []
                    
                    for issue in issues:
                        issues_by_checker[checker_type].append({
                            'table': table_name,
                            'type': issue.get('type', 'unknown'),
                            'description': issue.get('description', 'No description'),
                            'severity': issue.get('severity', 'medium')
                        })
        
        # 获取问题最多的表
        problematic_tables = sorted(
            [(table, count) for table, count in total_issues_by_table.items()],
            key=lambda x: x[1], reverse=True
        )[:10]  # 前10个问题最多的表
        
        # 创建图表（如果启用）
        charts = {}
        if self.show_plots:
            # 每个表的问题数图
            if total_issues_by_table:
                charts['issues_by_table_chart'] = self._create_issues_by_table_chart(total_issues_by_table)
            
            # 按检查器类型的问题数图
            if issues_by_checker:
                charts['issues_by_checker_chart'] = self._create_issues_by_checker_chart(issues_by_checker)
        
        return {
            'issues_by_checker': issues_by_checker,
            'total_issues_by_table': total_issues_by_table,
            'problematic_tables': problematic_tables,
            'charts': charts
        }
    
    def _create_optimization_section(self, optimization_results):
        """
        创建优化建议部分
        
        Args:
            optimization_results (dict): 优化建议
            
        Returns:
            dict: 优化建议部分内容
        """
        # 按表和优化类型汇总建议
        suggestions_by_table = {}
        suggestions_by_type = {
            'storage': [],
            'query': [],
            'maintenance': [],
            'schema': []
        }
        
        for table_name, table_suggestions in optimization_results.items():
            if table_name not in suggestions_by_table:
                suggestions_by_table[table_name] = []
            
            # 处理存储优化建议
            for suggestion in table_suggestions.get('storage_optimization', []):
                suggestions_by_table[table_name].append({
                    'type': 'storage',
                    'description': suggestion
                })
                suggestions_by_type['storage'].append({
                    'table': table_name,
                    'description': suggestion
                })
            
            # 处理查询优化建议
            for suggestion in table_suggestions.get('query_optimization', []):
                suggestions_by_table[table_name].append({
                    'type': 'query',
                    'description': suggestion
                })
                suggestions_by_type['query'].append({
                    'table': table_name,
                    'description': suggestion
                })
            
            # 处理维护建议
            for suggestion in table_suggestions.get('maintenance_recommendations', []):
                suggestions_by_table[table_name].append({
                    'type': 'maintenance',
                    'description': suggestion
                })
                suggestions_by_type['maintenance'].append({
                    'table': table_name,
                    'description': suggestion
                })
            
            # 处理架构改进建议
            for suggestion in table_suggestions.get('schema_improvements', []):
                suggestions_by_table[table_name].append({
                    'type': 'schema',
                    'description': suggestion
                })
                suggestions_by_type['schema'].append({
                    'table': table_name,
                    'description': suggestion
                })
        
        # 创建图表（如果启用）
        charts = {}
        if self.show_plots:
            # 按优化类型的建议数图
            suggestion_counts = {
                stype: len(suggestions) for stype, suggestions in suggestions_by_type.items()
            }
            if any(suggestion_counts.values()):
                charts['suggestions_by_type_chart'] = self._create_suggestions_by_type_chart(suggestion_counts)
        
        return {
            'suggestions_by_table': suggestions_by_table,
            'suggestions_by_type': suggestions_by_type,
            'charts': charts
        }
    
    def _create_table_summaries(self, tables_info, quality_results):
        """
        创建表摘要
        
        Args:
            tables_info (dict): 表信息
            quality_results (dict): 质量检查结果
            
        Returns:
            list: 表摘要列表
        """
        summaries = []
        
        for table_name, table_info in tables_info.items():
            # 基本信息
            summary = {
                'name': table_name,
                'row_count': table_info.get('info', {}).get('approximate_row_count', 0),
                'column_count': table_info.get('column_count', 0),
                'size': table_info.get('info', {}).get('total_size', 'unknown'),
                'columns': table_info.get('columns', []),
                'quality_results': quality_results.get(table_name, {})
            }
            
            # 计算问题数
            issue_count = 0
            for checker_type, results in summary['quality_results'].items():
                if isinstance(results, dict) and 'issues' in results:
                    issue_count += len(results.get('issues', []))
            
            summary['issue_count'] = issue_count
            
            # 添加到摘要列表
            summaries.append(summary)
        
        # 按问题数排序
        return sorted(summaries, key=lambda x: x['issue_count'], reverse=True)
    
    def _create_quality_summary(self, quality_results):
        """
        创建数据质量摘要
        
        Args:
            quality_results (dict): 质量检查结果
            
        Returns:
            dict: 质量摘要
        """
        # 计算总问题数
        total_issues = 0
        severe_issues = 0
        medium_issues = 0
        low_issues = 0
        
        # 汇总问题类型
        issue_types = {}
        
        for table_name, table_results in quality_results.items():
            for checker_type, results in table_results.items():
                if isinstance(results, dict) and 'issues' in results:
                    issues = results.get('issues', [])
                    total_issues += len(issues)
                    
                    for issue in issues:
                        # 按严重程度统计
                        severity = issue.get('severity', 'medium')
                        if severity == 'high':
                            severe_issues += 1
                        elif severity == 'medium':
                            medium_issues += 1
                        else:
                            low_issues += 1
                        
                        # 按类型统计
                        issue_type = issue.get('type', 'unknown')
                        if issue_type not in issue_types:
                            issue_types[issue_type] = 0
                        issue_types[issue_type] += 1
        
        # 计算质量评分（简单实现）
        if total_issues == 0:
            quality_score = 10
        else:
            # 基于问题数和严重程度的加权评分
            weighted_issues = severe_issues * 3 + medium_issues * 1.5 + low_issues * 0.5
            table_count = len(quality_results)
            if table_count == 0:
                table_count = 1
            
            # 计算平均每表的加权问题数
            avg_weighted_issues = weighted_issues / table_count
            
            # 转换为10分制评分
            quality_score = max(0, 10 - min(10, avg_weighted_issues))
        
        return {
            'total_issues': total_issues,
            'severe_issues': severe_issues,
            'medium_issues': medium_issues,
            'low_issues': low_issues,
            'issue_types': issue_types,
            'quality_score': round(quality_score, 1)
        }
    
    def _create_summary_data(self, context):
        """
        创建用于AI分析的摘要数据
        
        Args:
            context (dict): 报告上下文
            
        Returns:
            dict: 简化的摘要数据
        """
        return {
            'schema_name': context.get('schema_name'),
            'db_name': context.get('db_name'),
            'table_count': context.get('table_count'),
            'overview': {
                'summary_stats': context.get('report_sections')[0].get('content', {}).get('summary_stats', {}),
                'issue_summary': context.get('report_sections')[0].get('content', {}).get('issue_summary', {})
            },
            'quality_summary': context.get('quality_summary', {}),
            'problematic_tables': context.get('report_sections')[2].get('content', {}).get('problematic_tables', [])
        }
    
    def _create_table_size_chart(self, tables_info):
        """
        创建表大小分布图
        
        Args:
            tables_info (dict): 表信息
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 提取表大小数据
            table_names = []
            row_counts = []
            
            for table_name, table_info in tables_info.items():
                table_names.append(table_name)
                row_counts.append(table_info.get('info', {}).get('approximate_row_count', 0))
            
            # 限制显示的表数量
            if len(table_names) > 15:
                # 按行数排序并获取前15个
                sorted_data = sorted(zip(table_names, row_counts), key=lambda x: x[1], reverse=True)
                table_names = [d[0] for d in sorted_data[:14]]
                row_counts = [d[1] for d in sorted_data[:14]]
                # 添加"其他"类别
                other_count = sum(d[1] for d in sorted_data[14:])
                table_names.append('Others')
                row_counts.append(other_count)
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            bars = plt.bar(range(len(table_names)), row_counts, color=sns.color_palette('Blues_d', len(table_names)))
            plt.xticks(range(len(table_names)), table_names, rotation=45, ha='right')
            plt.ylabel('Rows')  # Changed to English
            plt.title('Table Size Distribution')  # Changed to English
            plt.tight_layout()
            
            # 添加数据标签
            for bar in bars:
                height = bar.get_height()
                plt.text(
                    bar.get_x() + bar.get_width() / 2.,
                    height,
                    f'{height:,}',
                    ha='center', va='bottom', rotation=0
                )
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建表大小分布图失败: {str(e)}")
            return ""
    
    def _create_issue_type_chart(self, issue_by_type):
        """
        创建问题类型分布图
        
        Args:
            issue_by_type (dict): 按类型统计的问题
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 提取数据
            types = list(issue_by_type.keys())
            counts = list(issue_by_type.values())
            
            # 限制显示的类型数量
            if len(types) > 10:
                # 按计数排序并获取前9个
                sorted_data = sorted(zip(types, counts), key=lambda x: x[1], reverse=True)
                types = [d[0] for d in sorted_data[:9]]
                counts = [d[1] for d in sorted_data[:9]]
                # 添加"其他"类别
                other_count = sum(d[1] for d in sorted_data[9:])
                types.append('Others')
                counts.append(other_count)
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            plt.pie(counts, labels=types, autopct='%1.1f%%', startangle=90, 
                   colors=sns.color_palette('Set3', len(types)))
            plt.axis('equal')  # 确保饼图是圆的
            plt.title('Issue Type Distribution')  # Changed to English
            plt.tight_layout()
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建问题类型分布图失败: {str(e)}")
            return ""
    
    def _create_column_type_chart(self, column_types):
        """
        创建列数据类型分布图
        
        Args:
            column_types (dict): 列数据类型统计
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 提取数据
            types = list(column_types.keys())
            counts = list(column_types.values())
            
            # 限制显示的类型数量
            if len(types) > 10:
                # 按计数排序并获取前9个
                sorted_data = sorted(zip(types, counts), key=lambda x: x[1], reverse=True)
                types = [d[0] for d in sorted_data[:9]]
                counts = [d[1] for d in sorted_data[:9]]
                # 添加"其他"类别
                other_count = sum(d[1] for d in sorted_data[9:])
                types.append('Others')
                counts.append(other_count)
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            plt.bar(range(len(types)), counts, color=sns.color_palette('Spectral', len(types)))
            plt.xticks(range(len(types)), types, rotation=45, ha='right')
            plt.ylabel('Columns')  # Changed to English
            plt.title('Column Data Type Distribution')  # Changed to English
            plt.tight_layout()
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建列数据类型分布图失败: {str(e)}")
            return ""
    
    def _create_relationship_chart(self, relationships, tables_info):
        """
        创建关系图
        
        Args:
            relationships (dict): 表关系
            tables_info (dict): 表信息
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 这里使用非常简化的关系图实现
            # 实际应用中可能需要使用networkx等库创建更复杂的图
            
            # 创建节点（表）和边（关系）
            nodes = list(tables_info.keys())
            edges = []
            
            for from_table, rels in relationships.items():
                for rel in rels:
                    # 排除反向关系以避免重复
                    if not rel.get('is_reverse'):
                        edges.append((from_table, rel['to_table']))
            
            # 限制表数量
            if len(nodes) > 20:
                logging.warning("表数量过多，跳过关系图生成")
                return ""
            
            # 创建图表
            plt.figure(figsize=(12, 10))
            
            # 创建一个简单的环形布局
            n = len(nodes)
            node_positions = {}
            for i, node in enumerate(nodes):
                angle = 2 * np.pi * i / n
                node_positions[node] = (np.cos(angle), np.sin(angle))
            
            # 绘制节点
            for node, pos in node_positions.items():
                plt.plot(pos[0], pos[1], 'o', markersize=10, 
                        color=sns.color_palette('husl', len(nodes))[nodes.index(node)])
                plt.text(pos[0], pos[1], node, fontsize=8, ha='center', va='center',
                        bbox=dict(facecolor='white', alpha=0.7, boxstyle='round'))
            
            # 绘制边
            for edge in edges:
                if edge[0] in node_positions and edge[1] in node_positions:
                    plt.plot(
                        [node_positions[edge[0]][0], node_positions[edge[1]][0]],
                        [node_positions[edge[0]][1], node_positions[edge[1]][1]],
                        '-', alpha=0.5, color='gray'
                    )
            
            plt.title('Table Relationship Diagram')  # Changed to English
            plt.axis('equal')
            plt.axis('off')
            plt.tight_layout()
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建关系图失败: {str(e)}")
            return ""
    
    def _create_issues_by_table_chart(self, total_issues_by_table):
        """
        创建每个表的问题数图
        
        Args:
            total_issues_by_table (dict): 每个表的问题总数
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 提取数据
            tables = list(total_issues_by_table.keys())
            issue_counts = list(total_issues_by_table.values())
            
            # 按问题数量排序
            sorted_data = sorted(zip(tables, issue_counts), key=lambda x: x[1], reverse=True)
            tables = [d[0] for d in sorted_data]
            issue_counts = [d[1] for d in sorted_data]
            
            # 限制显示的表数量
            if len(tables) > 15:
                tables = tables[:15]
                issue_counts = issue_counts[:15]
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            bars = plt.barh(range(len(tables)), issue_counts, 
                           color=sns.color_palette('Reds_r', len(tables)))
            plt.yticks(range(len(tables)), tables)
            plt.xlabel('Issues')  # Changed to English
            plt.title('Issues by Table')  # Changed to English
            plt.tight_layout()
            
            # 添加数据标签
            for bar in bars:
                width = bar.get_width()
                plt.text(
                    width + 0.5,
                    bar.get_y() + bar.get_height() / 2.,
                    f'{width:,}',
                    ha='left', va='center'
                )
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建表问题数图失败: {str(e)}")
            return ""
    
    def _create_issues_by_checker_chart(self, issues_by_checker):
        """
        创建按检查器类型的问题数图
        
        Args:
            issues_by_checker (dict): 按检查器类型的问题
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 计算每种检查器的问题数
            checker_counts = {checker: len(issues) for checker, issues in issues_by_checker.items()}
            
            # 提取数据
            checkers = list(checker_counts.keys())
            issue_counts = list(checker_counts.values())
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            plt.bar(range(len(checkers)), issue_counts, 
                   color=sns.color_palette('viridis', len(checkers)))
            plt.xticks(range(len(checkers)), [c.replace('_checker', '') for c in checkers], 
                      rotation=45, ha='right')
            plt.ylabel('Issues')  # Changed to English
            plt.title('Issues Found by Checker')  # Changed to English
            plt.tight_layout()
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建检查器问题数图失败: {str(e)}")
            return ""
    
    def _create_suggestions_by_type_chart(self, suggestion_counts):
        """
        创建按优化类型的建议数图
        
        Args:
            suggestion_counts (dict): 按类型的建议数
            
        Returns:
            str: Base64编码的图像
        """
        try:
            # 提取数据
            types = list(suggestion_counts.keys())
            counts = list(suggestion_counts.values())
            
            # 创建图表
            plt.figure(figsize=(8, 6))
            plt.pie(counts, labels=types, autopct='%1.1f%%', startangle=90,
                   colors=sns.color_palette('Paired', len(types)))
            plt.axis('equal')
            plt.title('Optimization Suggestion Distribution')  # Changed to English
            plt.tight_layout()
            
            # 转换为Base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close()
            
            return f"data:image/png;base64,{image_base64}"
        except Exception as e:
            logging.error(f"创建建议类型图失败: {str(e)}")
            return ""
    
    def _generate_html_report(self, context):
        """
        生成HTML报告
        
        Args:
            context (dict): 报告上下文
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 加载模板
            template = self.jinja_env.get_template('report_template.html')
            
            # 渲染HTML
            html_content = template.render(**context)
            
            # 保存文件
            file_path = os.path.join(self.output_path, f"{self.output_filename}.html")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logging.info(f"HTML报告已生成: {file_path}")
            return file_path
        except Exception as e:
            logging.error(f"生成HTML报告失败: {str(e)}")
            # 尝试创建基本报告
            return self._generate_basic_html_report(context)
    
    def _generate_basic_html_report(self, context):
        """
        生成基本HTML报告（当模板加载失败时使用）
        
        Args:
            context (dict): 报告上下文
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 创建基本HTML
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>PostgreSQL数据质量分析报告 - {context.get('schema_name')}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1, h2, h3 {{ color: #336699; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <h1>PostgreSQL数据质量分析报告</h1>
                <p>Schema: {context.get('schema_name')}</p>
                <p>数据库: {context.get('db_name')}</p>
                <p>报告生成时间: {context.get('timestamp')}</p>
                
                <h2>概述</h2>
                <p>总表数: {context.get('table_count')}</p>
                <p>总问题数: {context.get('quality_summary', {}).get('total_issues', 0)}</p>
                <p>质量评分: {context.get('quality_summary', {}).get('quality_score', 'N/A')}/10</p>
                
                <h2>表摘要</h2>
                <table>
                    <tr>
                        <th>表名</th>
                        <th>行数</th>
                        <th>列数</th>
                        <th>问题数</th>
                    </tr>
            """
            
            # 添加表摘要
            for table in context.get('table_summaries', []):
                html_content += f"""
                    <tr>
                        <td>{table.get('name')}</td>
                        <td>{table.get('row_count')}</td>
                        <td>{table.get('column_count')}</td>
                        <td>{table.get('issue_count')}</td>
                    </tr>
                """
            
            html_content += """
                </table>
            </body>
            </html>
            """
            
            # 保存文件
            file_path = os.path.join(self.output_path, f"{self.output_filename}_basic.html")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logging.info(f"基本HTML报告已生成: {file_path}")
            return file_path
        except Exception as e:
            logging.error(f"生成基本HTML报告失败: {str(e)}")
            return ""
    
    def _generate_markdown_report(self, context):
        """
        生成Markdown报告
        
        Args:
            context (dict): 报告上下文
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 加载模板
            template = self.jinja_env.get_template('report_template.md')
            
            # 渲染Markdown
            md_content = template.render(**context)
            
            # 保存文件
            file_path = os.path.join(self.output_path, f"{self.output_filename}.md")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            
            logging.info(f"Markdown报告已生成: {file_path}")
            return file_path
        except Exception as e:
            logging.error(f"生成Markdown报告失败: {str(e)}")
            # 尝试从HTML转换
            try:
                html_path = self._generate_html_report(context)
                if html_path:
                    with open(html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    
                    # 转换HTML到Markdown
                    markdown_content = md(html_content)
                    
                    # 保存文件
                    file_path = os.path.join(self.output_path, f"{self.output_filename}.md")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(markdown_content)
                    
                    logging.info(f"Markdown报告已从HTML转换生成: {file_path}")
                    return file_path
            except Exception as e2:
                logging.error(f"从HTML转换Markdown报告失败: {str(e2)}")
            
            return ""
    
    def _generate_json_report(self, context):
        """
        生成JSON报告
        
        Args:
            context (dict): 报告上下文
            
        Returns:
            str: 报告文件路径
        """
        try:
            # 清理上下文中的不可序列化对象
            clean_context = self._clean_for_json(context)
            
            # 保存文件
            file_path = os.path.join(self.output_path, f"{self.output_filename}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(clean_context, f, indent=2, ensure_ascii=False)
            
            logging.info(f"JSON报告已生成: {file_path}")
            return file_path
        except Exception as e:
            logging.error(f"生成JSON报告失败: {str(e)}")
            return ""
    
    def _clean_for_json(self, obj):
        """
        清理对象以便JSON序列化
        
        Args:
            obj: 要清理的对象
            
        Returns:
            清理后的对象
        """
        if isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items() if k != 'jinja_env'}
        elif isinstance(obj, list):
            return [self._clean_for_json(item) for item in obj]
        elif isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj
    
    def _create_default_templates(self, template_path):
        """
        创建默认报告模板
        
        Args:
            template_path (str): 模板目录路径
        """
        # 创建HTML模板
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>PostgreSQL数据质量分析报告 - {{ schema_name }}</title>
            <style>
                body {
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                }
                h1, h2, h3, h4 {
                    color: #2c3e50;
                    margin-top: 1.5em;
                }
                h1 {
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 10px;
                }
                table {
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px 0;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }
                th {
                    background-color: #f2f2f2;
                    font-weight: bold;
                }
                tr:nth-child(even) {
                    background-color: #f9f9f9;
                }
                .card {
                    border: 1px solid #ddd;
                    border-radius: 4px;
                    padding: 20px;
                    margin-bottom: 20px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .chart {
                    text-align: center;
                    margin: 20px 0;
                }
                .chart img {
                    max-width: 100%;
                    height: auto;
                }
                .stats {
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: space-between;
                }
                .stat-item {
                    flex: 0 0 30%;
                    background-color: #f8f9fa;
                    border-radius: 4px;
                    padding: 15px;
                    margin-bottom: 15px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }
                .stat-item h4 {
                    margin-top: 0;
                    color: #3498db;
                }
                .issue {
                    border-left: 4px solid #e74c3c;
                    padding-left: 15px;
                    margin-bottom: 10px;
                }
                .score {
                    font-size: 24px;
                    font-weight: bold;
                    color: #fff;
                    background-color: #2c3e50;
                    border-radius: 50%;
                    width: 50px;
                    height: 50px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    margin-right: 15px;
                }
                .score-container {
                    display: flex;
                    align-items: center;
                }
                .tabs {
                    display: flex;
                    flex-wrap: wrap;
                    margin-bottom: 20px;
                }
                .tab {
                    flex: 1;
                    cursor: pointer;
                    padding: 10px 20px;
                    text-align: center;
                    background-color: #f2f2f2;
                    border: 1px solid #ddd;
                    border-bottom: none;
                    border-radius: 4px 4px 0 0;
                    margin-right: 5px;
                }
                .tab.active {
                    background-color: #fff;
                    border-bottom: 3px solid #3498db;
                }
                .tab-content {
                    display: none;
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-radius: 0 0 4px 4px;
                }
                .tab-content.active {
                    display: block;
                }
            </style>
            <script>
                function openTab(evt, tabName) {
                    var i, tabcontent, tablinks;
                    tabcontent = document.getElementsByClassName("tab-content");
                    for (i = 0; i < tabcontent.length; i++) {
                        tabcontent[i].style.display = "none";
                    }
                    tablinks = document.getElementsByClassName("tab");
                    for (i = 0; i < tablinks.length; i++) {
                        tablinks[i].className = tablinks[i].className.replace(" active", "");
                    }
                    document.getElementById(tabName).style.display = "block";
                    evt.currentTarget.className += " active";
                }
                
                document.addEventListener("DOMContentLoaded", function() {
                    // Open the first tab by default
                    document.getElementsByClassName("tab")[0].click();
                });
            </script>
        </head>
        <body>
            <h1>PostgreSQL数据质量分析报告</h1>
            
            <div class="card">
                <h2>报告概述</h2>
                <p><strong>Schema:</strong> {{ schema_name }}</p>
                <p><strong>数据库:</strong> {{ db_name }}</p>
                <p><strong>报告生成时间:</strong> {{ timestamp }}</p>
                
                <div class="score-container">
                    <div class="score">{{ quality_summary.quality_score }}</div>
                    <div>
                        <h3 style="margin-top: 0;">数据质量评分</h3>
                        <p>基于发现的问题数量和严重程度</p>
                    </div>
                </div>
                
                <div class="stats">
                    <div class="stat-item">
                        <h4>表数量</h4>
                        <p>{{ table_count }}</p>
                    </div>
                    <div class="stat-item">
                        <h4>发现问题</h4>
                        <p>{{ quality_summary.total_issues }}</p>
                    </div>
                    <div class="stat-item">
                        <h4>高严重度问题</h4>
                        <p>{{ quality_summary.severe_issues }}</p>
                    </div>
                </div>
            </div>
            
            <div class="tabs">
                <button class="tab" onclick="openTab(event, 'overview')">概述</button>
                <button class="tab" onclick="openTab(event, 'schema')">Schema分析</button>
                <button class="tab" onclick="openTab(event, 'quality')">数据质量</button>
                <button class="tab" onclick="openTab(event, 'optimization')">优化建议</button>
                <button class="tab" onclick="openTab(event, 'tables')">表详情</button>
                {% if ai_insights %}
                <button class="tab" onclick="openTab(event, 'ai_insights')">AI见解</button>
                {% endif %}
            </div>
            
            <div id="overview" class="tab-content">
                <h2>数据库概述</h2>
                
                {% set overview = report_sections[0].content %}
                
                <div class="stats">
                    <div class="stat-item">
                        <h4>总行数</h4>
                        <p>{{ overview.summary_stats.total_rows }}</p>
                    </div>
                    <div class="stat-item">
                        <h4>总列数</h4>
                        <p>{{ overview.summary_stats.total_columns }}</p>
                    </div>
                    <div class="stat-item">
                        <h4>表关系数</h4>
                        <p>{{ overview.summary_stats.relationship_count }}</p>
                    </div>
                </div>
                
                {% if overview.charts.table_size_chart %}
                <div class="chart">
                    <h3>表大小分布</h3>
                    <img src="{{ overview.charts.table_size_chart }}" alt="表大小分布">
                </div>
                {% endif %}
                
                {% if overview.charts.issue_type_chart %}
                <div class="chart">
                    <h3>问题类型分布</h3>
                    <img src="{{ overview.charts.issue_type_chart }}" alt="问题类型分布">
                </div>
                {% endif %}
            </div>
            
            <div id="schema" class="tab-content">
                <h2>Schema分析</h2>
                
                {% set schema = report_sections[1].content %}
                
                <h3>最大的表</h3>
                <table>
                    <tr>
                        <th>表名</th>
                        <th>行数</th>
                        <th>列数</th>
                        <th>大小</th>
                    </tr>
                    {% for table in schema.table_sizes[:10] %}
                    <tr>
                        <td>{{ table.name }}</td>
                        <td>{{ table.row_count }}</td>
                        <td>{{ table.column_count }}</td>
                        <td>{{ table.total_size }}</td>
                    </tr>
                    {% endfor %}
                </table>
                
                <h3>表关系</h3>
                <p>关系总数: {{ schema.relationships.total_relationships }}</p>
                <p>有关系的表: {{ schema.relationships.tables_with_relationships }}</p>
                
                {% if schema.charts.column_type_chart %}
                <div class="chart">
                    <h3>列数据类型分布</h3>
                    <img src="{{ schema.charts.column_type_chart }}" alt="列数据类型分布">
                </div>
                {% endif %}
                
                {% if schema.charts.relationship_chart %}
                <div class="chart">
                    <h3>表关系图</h3>
                    <img src="{{ schema.charts.relationship_chart }}" alt="表关系图">
                </div>
                {% endif %}
            </div>
            
            <div id="quality" class="tab-content">
                <h2>数据质量分析</h2>
                
                {% set quality = report_sections[2].content %}
                
                <h3>问题最多的表</h3>
                <table>
                    <tr>
                        <th>表名</th>
                        <th>问题数</th>
                    </tr>
                    {% for table, count in quality.problematic_tables %}
                    <tr>
                        <td>{{ table }}</td>
                        <td>{{ count }}</td>
                    </tr>
                    {% endfor %}
                </table>
                
                <h3>常见问题类型</h3>
                <div class="card">
                    {% for checker_type, issues in quality.issues_by_checker.items() %}
                    <h4>{{ checker_type|replace('_checker', '')|title }}</h4>
                    <ul>
                        {% for issue in issues[:5] %}
                        <li class="issue">
                            <strong>{{ issue.table }}:</strong> {{ issue.description }}
                        </li>
                        {% endfor %}
                        {% if issues|length > 5 %}
                        <li>... 及其他 {{ issues|length - 5 }} 个问题</li>
                        {% endif %}
                    </ul>
                    {% endfor %}
                </div>
                
                {% if quality.charts.issues_by_table_chart %}
                <div class="chart">
                    <h3>各表问题数量</h3>
                    <img src="{{ quality.charts.issues_by_table_chart }}" alt="各表问题数量">
                </div>
                {% endif %}
                
                {% if quality.charts.issues_by_checker_chart %}
                <div class="chart">
                    <h3>各检查器发现的问题数量</h3>
                    <img src="{{ quality.charts.issues_by_checker_chart }}" alt="各检查器发现的问题数量">
                </div>
                {% endif %}
            </div>
            
            <div id="optimization" class="tab-content">
                <h2>优化建议</h2>
                
                {% set optimization = report_sections[3].content %}
                
                <h3>优化建议概述</h3>
                
                {% for stype, suggestions in optimization.suggestions_by_type.items() %}
                <div class="card">
                    <h4>{{ stype|title }} 优化 ({{ suggestions|length }})</h4>
                    <ul>
                        {% for suggestion in suggestions[:5] %}
                        <li>
                            <strong>{{ suggestion.table }}:</strong> {{ suggestion.description }}
                        </li>
                        {% endfor %}
                        {% if suggestions|length > 5 %}
                        <li>... 及其他 {{ suggestions|length - 5 }} 个建议</li>
                        {% endif %}
                    </ul>
                </div>
                {% endfor %}
                
                {% if optimization.charts.suggestions_by_type_chart %}
                <div class="chart">
                    <h3>优化建议类型分布</h3>
                    <img src="{{ optimization.charts.suggestions_by_type_chart }}" alt="优化建议类型分布">
                </div>
                {% endif %}
            </div>
            
            <div id="tables" class="tab-content">
                <h2>表详情</h2>
                
                <div class="tabs">
                    {% for table in table_summaries %}
                    <button class="tab" onclick="openTab(event, 'table_{{ loop.index }}')">{{ table.name }}</button>
                    {% if loop.index % 10 == 0 and not loop.last %}
                    </div><div class="tabs">
                    {% endif %}
                    {% endfor %}
                </div>
                
                {% for table in table_summaries %}
                <div id="table_{{ loop.index }}" class="tab-content">
                    <h3>{{ table.name }}</h3>
                    
                    <div class="stats">
                        <div class="stat-item">
                            <h4>行数</h4>
                            <p>{{ table.row_count }}</p>
                        </div>
                        <div class="stat-item">
                            <h4>列数</h4>
                            <p>{{ table.column_count }}</p>
                        </div>
                        <div class="stat-item">
                            <h4>问题数</h4>
                            <p>{{ table.issue_count }}</p>
                        </div>
                    </div>
                    
                    <h4>列信息</h4>
                    <table>
                        <tr>
                            <th>列名</th>
                            <th>数据类型</th>
                            <th>可空</th>
                            <th>描述</th>
                        </tr>
                        {% for column in table.columns %}
                        <tr>
                            <td>{{ column.column_name }}</td>
                            <td>{{ column.data_type }}</td>
                            <td>{{ column.is_nullable }}</td>
                            <td>{{ column.column_description }}</td>
                        </tr>
                        {% endfor %}
                    </table>
                    
                    {% if table.quality_results %}
                    <h4>发现的问题</h4>
                    <ul>
                        {% for checker_type, results in table.quality_results.items() %}
                            {% if results.issues %}
                                {% for issue in results.issues %}
                                <li class="issue">
                                    <strong>{{ checker_type|replace('_checker', '')|title }}:</strong> {{ issue.description }}
                                </li>
                                {% endfor %}
                            {% endif %}
                        {% endfor %}
                    </ul>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
            
            {% if ai_insights %}
            <div id="ai_insights" class="tab-content">
                <h2>AI生成的见解</h2>
                
                {% if ai_insights.error %}
                <p>获取AI见解时出错: {{ ai_insights.error }}</p>
                {% else %}
                
                {% if ai_insights.quality_assessment %}
                <div class="card">
                    <h3>数据质量评估</h3>
                    <div class="score-container">
                        <div class="score">{{ ai_insights.quality_assessment.score }}</div>
                        <div>
                            <p>{{ ai_insights.quality_assessment.summary }}</p>
                        </div>
                    </div>
                </div>
                {% endif %}
                
                {% if ai_insights.key_issues %}
                <div class="card">
                    <h3>关键问题</h3>
                    <table>
                        <tr>
                            <th>问题</th>
                            <th>影响</th>
                            <th>优先级</th>
                        </tr>
                        {% for issue in ai_insights.key_issues %}
                        <tr>
                            <td>{{ issue.issue }}</td>
                            <td>{{ issue.impact }}</td>
                            <td>{{ issue.priority }}</td>
                        </tr>
                        {% endfor %}
                    </table>
                </div>
                {% endif %}
                
                {% if ai_insights.patterns %}
                <div class="card">
                    <h3>趋势和模式</h3>
                    <ul>
                        {% for pattern in ai_insights.patterns %}
                        <li>{{ pattern }}</li>
                        {% endfor %}
                    </ul>
                </div>
                {% endif %}
                
                {% if ai_insights.improvement_steps %}
                <div class="card">
                    <h3>改进步骤</h3>
                    {% for item in ai_insights.improvement_steps %}
                    <h4>{{ item.issue }}</h4>
                    <ol>
                        {% for step in item.steps %}
                        <li>{{ step }}</li>
                        {% endfor %}
                    </ol>
                    {% endfor %}
                </div>
                {% endif %}
                
                {% if ai_insights.best_practices %}
                <div class="card">
                    <h3>最佳实践建议</h3>
                    <ul>
                        {% for practice in ai_insights.best_practices %}
                        <li>{{ practice }}</li>
                        {% endfor %}
                    </ul>
                </div>
                {% endif %}
                
                {% endif %}
            </div>
            {% endif %}
            
            <div style="margin-top: 30px; text-align: center; color: #777; font-size: 0.8em;">
                <p>由PostgreSQL数据质量分析工具生成</p>
            </div>
        </body>
        </html>
        """
        
        with open(os.path.join(template_path, 'report_template.html'), 'w', encoding='utf-8') as f:
            f.write(html_template)
        
        # 创建Markdown模板
        md_template = """
        # PostgreSQL数据质量分析报告

        **Schema:** {{ schema_name }}  
        **数据库:** {{ db_name }}  
        **生成时间:** {{ timestamp }}

        ## 概述

        质量评分: **{{ quality_summary.quality_score }}/10**

        - 表数量: {{ table_count }}
        - 发现问题: {{ quality_summary.total_issues }}
        - 高严重度问题: {{ quality_summary.severe_issues }}

        {% set overview = report_sections[0].content %}

        - 总行数: {{ overview.summary_stats.total_rows }}
        - 总列数: {{ overview.summary_stats.total_columns }}
        - 表关系数: {{ overview.summary_stats.relationship_count }}

        ## 数据质量问题

        {% set quality = report_sections[2].content %}

        ### 问题最多的表

        {% for table, count in quality.problematic_tables[:5] %}
        - **{{ table }}**: {{ count }} 个问题
        {% endfor %}

        ### 常见问题类型

        {% for checker_type, issues in quality.issues_by_checker.items() %}
        #### {{ checker_type|replace('_checker', '')|title }}

        {% for issue in issues[:5] %}
        - **{{ issue.table }}**: {{ issue.description }}
        {% endfor %}
        {% if issues|length > 5 %}
        - ... 及其他 {{ issues|length - 5 }} 个问题
        {% endif %}

        {% endfor %}

        ## 优化建议

        {% set optimization = report_sections[3].content %}

        {% for stype, suggestions in optimization.suggestions_by_type.items() %}
        ### {{ stype|title }} 优化

        {% for suggestion in suggestions[:5] %}
        - **{{ suggestion.table }}**: {{ suggestion.description }}
        {% endfor %}
        {% if suggestions|length > 5 %}
        - ... 及其他 {{ suggestions|length - 5 }} 个建议
        {% endif %}

        {% endfor %}

        ## 表详情

        {% for table in table_summaries[:10] %}
        ### {{ table.name }}

        - 行数: {{ table.row_count }}
        - 列数: {{ table.column_count }}
        - 问题数: {{ table.issue_count }}

        #### 发现的问题

        {% if table.quality_results %}
        {% for checker_type, results in table.quality_results.items() %}
        {% if results.issues %}
        {% for issue in results.issues %}
        - **{{ checker_type|replace('_checker', '')|title }}**: {{ issue.description }}
        {% endfor %}
        {% endif %}
        {% endfor %}
        {% else %}
        - 未发现问题
        {% endif %}

        {% endfor %}

        {% if table_summaries|length > 10 %}
        *还有 {{ table_summaries|length - 10 }} 个表未在此显示*
        {% endif %}

        {% if ai_insights and not ai_insights.error %}
        ## AI见解

        {% if ai_insights.quality_assessment %}
        ### 数据质量评估

        **评分:** {{ ai_insights.quality_assessment.score }}

        {{ ai_insights.quality_assessment.summary }}
        {% endif %}

        {% if ai_insights.key_issues %}
        ### 关键问题

        {% for issue in ai_insights.key_issues %}
        - **{{ issue.issue }}** ({{ issue.priority }}): {{ issue.impact }}
        {% endfor %}
        {% endif %}

        {% if ai_insights.improvement_steps %}
        ### 改进步骤

        {% for item in ai_insights.improvement_steps %}
        #### {{ item.issue }}

        {% for step in item.steps %}
        1. {{ step }}
        {% endfor %}

        {% endfor %}
        {% endif %}
        {% endif %}

        ---

        *由PostgreSQL数据质量分析工具生成*
        """
        
        with open(os.path.join(template_path, 'report_template.md'), 'w', encoding='utf-8') as f:
            f.write(md_template)