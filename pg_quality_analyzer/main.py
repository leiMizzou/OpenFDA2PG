"""
PostgreSQL数据质量检查与分析程序主模块
"""
import argparse
import logging
import os
import sys
import json
from datetime import datetime

from config import Config
from db_connector import DBConnector
from schema_analyzer import SchemaAnalyzer
from data_sampler import DataSampler
from data_type_detector import DataTypeDetector, ensure_nltk_resources
from quality_checker import (
    NullChecker, DistributionChecker, ConsistencyChecker, 
    UnstructuredChecker, CustomChecker
)
from unstructured_analyzer import UnstructuredAnalyzer
from preprocessing_advisor import PreprocessingAdvisor
from gemini_integrator import GeminiIntegrator
from optimization_analyzer import OptimizationAnalyzer
from report_generator import ReportGenerator

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    
    # 加载配置
    config = Config(args.config)
    
    # 若指定了命令行连接参数，则更新配置
    if args.host:
        config.set('database.host', args.host)
    if args.port:
        config.set('database.port', args.port)
    if args.user:
        config.set('database.user', args.user)
    if args.password:
        config.set('database.password', args.password)
    if args.dbname:
        config.set('database.dbname', args.dbname)
    if args.schema:
        config.set('database.schema', args.schema)
    
    # 设置日志
    setup_logging(config)
    
    # 显示欢迎信息
    print_welcome()
    
    # 确保NLTK资源可用
    try:
        ensure_nltk_resources()
    except Exception as e:
        logging.warning(f"NLTK资源准备失败: {str(e)}，但程序将继续执行")
    
    # 连接数据库
    db = DBConnector(config)
    if not db.connect():
        logging.error("无法连接到数据库，程序退出")
        return 1
    
    try:
        # 初始化Gemini API集成器（如果启用）
        gemini = None
        if config.get('gemini.enable') and config.get('gemini.api_key'):
            gemini = GeminiIntegrator(config)
            if gemini.is_available():
                logging.info("Gemini API集成已启用")
            else:
                logging.warning("Gemini API集成未就绪，将不使用AI增强功能")
                gemini = None
        
        # 执行数据质量检查和分析
        result = run_analysis(config, db, gemini)
        
        # 生成报告
        generate_report(config, result, gemini)
        
        return 0
    
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {str(e)}", exc_info=True)
        return 1
    
    finally:
        # 关闭数据库连接
        db.disconnect()

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='PostgreSQL数据质量检查与分析程序')
    
    # 配置文件
    parser.add_argument('--config', '-c', 
                      help='配置文件路径', 
                      default='config.yaml')
    
    # 数据库连接参数
    parser.add_argument('--host', 
                      help='数据库主机地址')
    parser.add_argument('--port', type=int,
                      help='数据库端口')
    parser.add_argument('--user', '-u',
                      help='数据库用户名')
    parser.add_argument('--password', '-p',
                      help='数据库密码')
    parser.add_argument('--dbname', '-d',
                      help='数据库名称')
    parser.add_argument('--schema', '-s',
                      help='Schema名称')
    
    # 输出格式和位置
    parser.add_argument('--output-format', 
                      choices=['html', 'markdown', 'json'],
                      help='报告输出格式')
    parser.add_argument('--output-path',
                      help='报告输出路径')
    
    # 其他选项
    parser.add_argument('--max-tables', type=int,
                      help='最大分析表数量')
    parser.add_argument('--sample-size', type=int,
                      help='每表采样大小')
    parser.add_argument('--no-ai', action='store_true',
                      help='禁用AI增强功能')
    
    return parser.parse_args()

def setup_logging(config):
    """设置日志"""
    log_level = getattr(logging, config.get('logging.level').upper())
    log_file = config.get('logging.file')
    
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def print_welcome():
    """显示欢迎信息"""
    print("""
╔════════════════════════════════════════════════════════╗
║                                                        ║
║      PostgreSQL数据质量检查与分析程序                  ║
║                                                        ║
╚════════════════════════════════════════════════════════╝
    """)

def run_analysis(config, db, gemini=None):
    """
    执行数据质量分析
    
    Args:
        config (Config): 配置对象
        db (DBConnector): 数据库连接器
        gemini (GeminiIntegrator, optional): Gemini集成器
        
    Returns:
        dict: 分析结果
    """
    logging.info("开始数据质量分析")
    
    # 分析Schema
    logging.info("开始分析Schema")
    schema_analyzer = SchemaAnalyzer(db, config)
    schema_analysis = schema_analyzer.analyze_schema()
    
    schema_info = schema_analysis.get('schema', {})
    tables_info = schema_analysis.get('tables', {})
    relationships = schema_analysis.get('relationships', {})
    
    # 从表中采样数据
    logging.info("开始从表中采样数据")
    data_sampler = DataSampler(db, config)
    table_samples = data_sampler.sample_tables(list(tables_info.keys()))
    
    # 数据类型检测
    logging.info("开始检测数据类型")
    type_detector = DataTypeDetector(config)
    
    # 质量检查器初始化
    checkers = {
        'null_checker': NullChecker(config),
        'distribution_checker': DistributionChecker(config),
        'consistency_checker': ConsistencyChecker(config),
        'unstructured_checker': UnstructuredChecker(config, gemini)
    }
    
    # 如果启用了Gemini，添加自定义检查器
    if gemini and config.get('gemini.custom_check_generation'):
        checkers['custom_checker'] = CustomChecker(config, gemini)
    
    # 执行质量检查
    logging.info("开始执行质量检查")
    quality_results = {}
    
    for table_name, df in table_samples.items():
        logging.info(f"检查表 {table_name}")
        table_info = tables_info.get(table_name, {})
        
        # 提取列信息
        columns_info = {}
        for column in table_info.get('columns', []):
            column_name = column.get('column_name')
            if column_name:
                columns_info[column_name] = column
        
        # 检测列数据类型
        try:
            column_types = type_detector.analyze_table_columns(df)
        except Exception as e:
            logging.error(f"表 {table_name} 数据类型检测失败: {str(e)}")
            column_types = {}
        
        # 表级质量检查结果
        table_results = {}
        
        # 对每个检查器执行检查，并处理可能的异常
        for checker_name, checker in checkers.items():
            if checker.is_enabled():
                try:
                    # 执行检查
                    if checker_name == 'consistency_checker':
                        # 一致性检查需要表关系信息
                        result = checker.check_table(table_name, df, columns_info, relationships.get(table_name, []))
                    else:
                        result = checker.check_table(table_name, df, columns_info)
                    
                    table_results[checker_name] = result
                except Exception as e:
                    logging.error(f"检查器 {checker_name} 在表 {table_name} 上执行失败: {str(e)}")
                    table_results[checker_name] = {'error': str(e), 'issues': []}
        
        quality_results[table_name] = table_results
    
    # 检查表间关系的一致性
    if 'consistency_checker' in checkers and checkers['consistency_checker'].is_enabled():
        logging.info("检查表间关系一致性")
        try:
            consistency_checker = checkers['consistency_checker']
            relationship_results = consistency_checker.check_relationships(
                table_samples, relationships, schema_info
            )
            
            # 将关系检查结果添加到相应表的结果中
            for table_name, rel_results in relationship_results.items():
                if table_name in quality_results:
                    if 'consistency_checker' in quality_results[table_name]:
                        quality_results[table_name]['consistency_checker']['relationship_checks'] = rel_results
                    else:
                        quality_results[table_name]['consistency_checker'] = {
                            'relationship_checks': rel_results
                        }
        except Exception as e:
            logging.error(f"检查表间关系一致性失败: {str(e)}")
    
    # 分析非结构化数据
    unstructured_results = {}
    preprocessor_results = {}
    
    if config.get('quality_checks.checks.unstructured_analysis'):
        logging.info("开始分析非结构化数据")
        unstructured_analyzer = UnstructuredAnalyzer(config, gemini)
        preprocessing_advisor = PreprocessingAdvisor(config, gemini)
        
        for table_name, df in table_samples.items():
            table_info = tables_info.get(table_name, {})
            table_unstructured_results = {}
            table_preprocessor_results = {}
            
            # 获取表中的所有列
            columns = table_info.get('columns', [])
            
            for column in columns:
                column_name = column.get('column_name')
                if not column_name or column_name not in df.columns:
                    continue
                
                # 获取列的类型分析结果
                type_info = column_types.get(column_name, {})
                is_structured = type_info.get('is_structured', True)
                
                # 如果是非结构化列，进行深入分析
                if not is_structured:
                    logging.info(f"分析非结构化列 {table_name}.{column_name}")
                    
                    try:
                        # 获取列数据
                        column_data = df[column_name].dropna().astype(str).tolist()
                        
                        # 列上下文信息
                        column_context = {
                            'table': table_name,
                            'column': column_name,
                            'data_type': column.get('data_type')
                        }
                        
                        # 执行非结构化分析
                        data_type = type_info.get('format_type', 'text')
                        
                        if data_type == 'json':
                            analysis = unstructured_analyzer.analyze_json(column_data, column_context)
                        elif data_type == 'text':
                            analysis = unstructured_analyzer.analyze_text(column_data, column_context)
                        else:
                            analysis = unstructured_analyzer.analyze_binary(column_data, column_context)
                        
                        table_unstructured_results[column_name] = analysis
                        
                        # 获取预处理建议
                        quality_result = quality_results.get(table_name, {}).get('unstructured_checker', {})
                        column_quality = quality_result.get('column_results', {}).get(column_name, {})
                        
                        preprocessing = preprocessing_advisor.recommend_strategies(
                            column, 
                            {'unstructured': {'data_type': data_type, 'analysis': analysis}},
                            column_data[:10]  # 提供少量样本数据
                        )
                        
                        table_preprocessor_results[column_name] = preprocessing
                    except Exception as e:
                        logging.error(f"分析非结构化列 {table_name}.{column_name} 失败: {str(e)}")
                        table_unstructured_results[column_name] = {'error': str(e)}
                        table_preprocessor_results[column_name] = {'error': str(e)}
            
            if table_unstructured_results:
                unstructured_results[table_name] = table_unstructured_results
            
            if table_preprocessor_results:
                preprocessor_results[table_name] = table_preprocessor_results
    
    # 生成优化建议
    logging.info("开始生成优化建议")
    optimization_analyzer = OptimizationAnalyzer(config, gemini)
    try:
        optimization_results = optimization_analyzer.analyze_optimization(
            tables_info, quality_results, schema_info
        )
    except Exception as e:
        logging.error(f"生成优化建议失败: {str(e)}")
        optimization_results = {}
    
    # 整合所有结果
    result = {
        'schema_info': schema_info,
        'tables_info': tables_info,
        'quality_results': quality_results,
        'unstructured_results': unstructured_results,
        'preprocessor_results': preprocessor_results,
        'optimization_results': optimization_results,
        'analysis_timestamp': datetime.now().isoformat()
    }
    
    logging.info("数据质量分析完成")
    return result

def generate_report(config, result, gemini=None):
    """
    生成分析报告
    
    Args:
        config (Config): 配置对象
        result (dict): 分析结果
        gemini (GeminiIntegrator, optional): Gemini集成器
        
    Returns:
        str: 报告文件路径
    """
    logging.info("开始生成报告")
    
    # 保存原始分析结果
    output_path = config.get('output.path', './reports')
    os.makedirs(output_path, exist_ok=True)
    
    # 保存原始JSON结果文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_file = os.path.join(output_path, f"raw_analysis_{timestamp}.json")
    
    try:
        with open(raw_file, 'w', encoding='utf-8') as f:
            # 清理不可序列化的对象
            clean_result = clean_for_json(result)
            json.dump(clean_result, f, indent=2, ensure_ascii=False)
        
        logging.info(f"原始分析结果已保存至 {raw_file}")
    except Exception as e:
        logging.error(f"保存原始分析结果失败: {str(e)}")
    
    # 初始化报告生成器
    try:
        report_generator = ReportGenerator(config, gemini)
        
        # 生成报告
        report_file = report_generator.generate_report(result)
        
        if report_file:
            logging.info(f"分析报告已生成: {report_file}")
            print(f"\n分析报告已生成: {report_file}")
        else:
            logging.error("报告生成失败")
            
        return report_file
    except Exception as e:
        logging.error(f"报告生成失败: {str(e)}")
        return None

def clean_for_json(obj):
    """
    清理对象以便JSON序列化
    
    Args:
        obj: 要清理的对象
        
    Returns:
        dict: 清理后的对象
    """
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items() 
                if k not in ['db', 'conn', 'cursor', 'jinja_env']}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif hasattr(obj, 'to_dict'):
        return clean_for_json(obj.to_dict())
    elif hasattr(obj, '__dict__'):
        return clean_for_json(obj.__dict__)
    else:
        try:
            # 尝试JSON序列化，不行就转字符串
            json.dumps(obj)
            return obj
        except (TypeError, OverflowError):
            return str(obj)

if __name__ == "__main__":
    sys.exit(main())