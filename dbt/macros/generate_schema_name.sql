{#-
  覆盖 dbt 默认的 schema 命名规则。

  dbt 默认会把自定义 schema 拼在目标 schema 后面（schema=silver + +schema:gold 会得到 silver_gold）。
  本项目的分层命名是固定的（OWD/OWS 落 silver、ADS 落 gold），因此直接用自定义 schema 本身。
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
