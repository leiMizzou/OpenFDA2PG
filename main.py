"""
FDA医疗设备数据库创建与数据导入主程序
"""
import json
import pandas as pd
from IPython.display import display, HTML

from config import DB_CONFIG, DATA_DIRS
from logger import show_header, show_version_info, log_info, log_error, log_warning, log_success
from file_handler import FileHandler
from schema_creator import SchemaCreator
from data_validator import DataValidator

from importers.classification_importer import ClassificationImporter
from importers.recall_importer import RecallImporter
from importers.enforcement_importer import EnforcementImporter
from importers.adverse_event_importer import AdverseEventImporter
from importers.udi_importer import UDIImporter

def main():
    """主程序执行流程"""
    try:
        # 显示应用程序标题
        show_header()
        
        # 显示版本和时间戳
        show_version_info()
        
        # 步骤1: 创建数据库模式
        creator = SchemaCreator(DB_CONFIG)
        if not creator.connect():
            raise Exception("无法连接到数据库")

        schema_created = creator.create_schema()
        creator.close()

        if not schema_created:
            raise Exception("无法创建数据库模式")

        # 获取所有文件列表
        files_classification = FileHandler.get_classification_files(DATA_DIRS['classification_dir'])
        files_enforcement = FileHandler.get_enforcement_files(DATA_DIRS['enforcement_dir'])
        files_event = FileHandler.get_event_files(DATA_DIRS['event_dir'])
        files_recall = FileHandler.get_recall_files(DATA_DIRS['recall_dir'])
        files_udi = FileHandler.get_udi_files(DATA_DIRS['udi_dir'])

        # 显示文件统计信息
        file_stats = [
            {"类型": "设备分类", "文件数量": len(files_classification)},
            {"类型": "执法行动", "文件数量": len(files_enforcement)},
            {"类型": "设备召回", "文件数量": len(files_recall)},
            {"类型": "不良事件报告", "文件数量": len(files_event)},
            {"类型": "UDI记录", "文件数量": len(files_udi)}
        ]

        display(HTML("<h3>文件统计信息</h3>"))
        display(pd.DataFrame(file_stats))

        # 检查元数据
        meta_data = []
        meta_sources = {
            '分类数据': files_classification[0] if files_classification else None,
            '执法数据': files_enforcement[0] if files_enforcement else None,
            '召回数据': files_recall[0] if files_recall else None,
            '事件数据': files_event[0] if files_event else None,
            'UDI数据': files_udi[0] if files_udi else None
        }
        
        for source_name, file_path in meta_sources.items():
            if file_path:
                meta = FileHandler.extract_meta_data(file_path)
                if meta and 'last_updated' in meta:
                    meta_data.append({
                        '数据类型': source_name,
                        '最后更新': meta['last_updated'],
                        '来源': 'FDA OpenFDA API'
                    })
        
        if meta_data:
            display(HTML("<h3>数据元信息</h3>"))
            display(pd.DataFrame(meta_data))

        # 查看文件样例结构 - 每种类型都显示一个
        sample_display = False
        
        if files_classification:
            classification_sample = FileHandler.sample_data(files_classification[0], record_count=1)
            if classification_sample:
                display(HTML("<h3>设备分类数据样例</h3>"))
                display(HTML(f"<pre>{json.dumps(classification_sample[0], indent=2)}</pre>"))
                sample_display = True
        
        if not sample_display and files_enforcement:
            enforcement_sample = FileHandler.sample_data(files_enforcement[0], record_count=1)
            if enforcement_sample:
                display(HTML("<h3>执法行动数据样例</h3>"))
                display(HTML(f"<pre>{json.dumps(enforcement_sample[0], indent=2)}</pre>"))
                sample_display = True
                
        # 让用户确认继续
        display(HTML("<h3>数据导入确认</h3>"))
        display(HTML("<p>以上是将要导入的数据概览。请确认数据来源和结构是否正确。</p>"))
        display(HTML("<p>如果正确，可以继续执行导入过程。数据导入可能需要较长时间，请耐心等待。</p>"))

        # 设置批处理大小以适应内存限制
        batch_size = 500

        # 创建导入器实例并连接数据库
        classification_importer = ClassificationImporter(DB_CONFIG)
        if not classification_importer.connect():
            raise Exception("无法连接到数据库")

        recall_importer = RecallImporter(DB_CONFIG)
        if not recall_importer.connect():
            raise Exception("无法连接到数据库")

        enforcement_importer = EnforcementImporter(DB_CONFIG)
        if not enforcement_importer.connect():
            raise Exception("无法连接到数据库")

        adverse_event_importer = AdverseEventImporter(DB_CONFIG)
        if not adverse_event_importer.connect():
            raise Exception("无法连接到数据库")

        udi_importer = UDIImporter(DB_CONFIG)
        if not udi_importer.connect():
            raise Exception("无法连接到数据库")

        # 导入设备分类数据
        if files_classification:
            display(HTML("<h3>正在导入设备分类数据...</h3>"))
            classification_count = classification_importer.import_data(files_classification, batch_size=batch_size)
            display(HTML(f"<p>成功导入 <b>{classification_count}</b> 条设备分类记录</p>"))
        else:
            log_warning("未找到设备分类文件")

        # 导入执法行动数据
        if files_enforcement:
            display(HTML("<h3>正在导入执法行动数据...</h3>"))
            enforcement_count = enforcement_importer.import_data(files_enforcement, batch_size=batch_size)
            display(HTML(f"<p>成功导入 <b>{enforcement_count}</b> 条执法行动记录</p>"))
        else:
            log_warning("未找到执法行动文件")
            
        # 导入设备召回数据
        if files_recall:
            display(HTML("<h3>正在导入设备召回数据...</h3>"))
            recall_count = recall_importer.import_data(files_recall, batch_size=batch_size)
            display(HTML(f"<p>成功导入 <b>{recall_count}</b> 条设备召回记录</p>"))
        else:
            log_warning("未找到设备召回文件")

        # 导入不良事件数据
        if files_event:
            display(HTML("<h3>正在导入不良事件数据...</h3>"))
            event_count = adverse_event_importer.import_data(files_event, batch_size=batch_size)
            display(HTML(f"<p>成功导入 <b>{event_count}</b> 条不良事件记录</p>"))
        else:
            log_warning("未找到不良事件报告文件")

        # 导入UDI数据
        if files_udi:
            display(HTML("<h3>正在导入UDI数据...</h3>"))
            udi_count = udi_importer.import_data(files_udi, batch_size=batch_size)
            display(HTML(f"<p>成功导入 <b>{udi_count}</b> 条UDI记录</p>"))
        else:
            log_warning("未找到UDI文件")

        # 关闭所有导入器连接
        classification_importer.close()
        recall_importer.close()
        enforcement_importer.close()
        adverse_event_importer.close()
        
        # 使用数据验证器验证数据
        display(HTML("<h3>正在验证导入的数据...</h3>"))
        # 创建新连接用于验证
        validator_importer = UDIImporter(DB_CONFIG)  # 仅用来获取连接
        if not validator_importer.connect():
            raise Exception("无法连接到数据库进行验证")
            
        validator = DataValidator(validator_importer.conn, validator_importer.cur)
        validation_success = validator.validate()
        validator_importer.close()

        if validation_success:
            display(HTML("<h3 style='color:green'>✅ 数据导入和验证全部完成</h3>"))
            display(HTML("<p>FDA医疗设备数据已成功导入到数据库中。现在可以使用SQL查询或其他工具来分析这些数据。</p>"))
            
            # 提供一些示例查询
            display(HTML("<h3>示例查询</h3>"))
            display(HTML("""
            <p>以下是一些示例SQL查询，可以帮助您开始分析数据：</p>
            <ol>
                <li>查询所有Class 3高风险设备：<br><code>SELECT * FROM device.product_codes WHERE device_class = '3';</code></li>
                <li>查询最近一年的不良事件趋势：<br><code>SELECT date_trunc('month', date_received) as month, COUNT(*) FROM device.adverse_events WHERE date_received > CURRENT_DATE - INTERVAL '1 year' GROUP BY month ORDER BY month;</code></li>
                <li>查询召回分类统计：<br><code>SELECT classification, COUNT(*) FROM device.device_recalls GROUP BY classification ORDER BY COUNT(*) DESC;</code></li>
                <li>查询特定医疗专业的设备：<br><code>SELECT pc.* FROM device.product_codes pc JOIN device.medical_specialties ms ON pc.medical_specialty_id = ms.id WHERE ms.code = 'CV';</code></li>
                <li>查询特定产品代码的所有相关记录：<br><code>SELECT 'classification' as type, dc.id FROM device.device_classifications dc WHERE dc.product_code = 'XXX' UNION ALL SELECT 'recall' as type, dr.id FROM device.device_recalls dr WHERE dr.product_code = 'XXX';</code></li>
            </ol>
            """))
        else:
            display(HTML("<h3 style='color:red'>❌ 数据验证失败，请检查导入日志</h3>"))
            
    except Exception as e:
        log_error(f"执行过程中发生错误: {str(e)}")
        display(HTML(f"<h3 style='color:red'>❌ 执行失败: {str(e)}</h3>"))

if __name__ == "__main__":
    main()
