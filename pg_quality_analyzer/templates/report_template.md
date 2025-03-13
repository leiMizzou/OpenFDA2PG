
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
        