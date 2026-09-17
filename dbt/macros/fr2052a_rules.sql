{#-
  FR 2052a 业务规则宏。

  把"什么算到期分桶、什么算 HQLA"这类监管口径集中在一处，
  六张 OWD 表共用同一份实现，避免各写一遍导致口径不一致。
-#}

{#- 到期分桶：按剩余天数归入 ref_maturity_bucket 的分桶编码 -#}
{% macro maturity_bucket(days_expr) -%}
    case
        when {{ days_expr }} is null then 'OPEN'
        when {{ days_expr }} <= 0 then 'O/N'
        when {{ days_expr }} <= 7 then '1-7D'
        when {{ days_expr }} <= 30 then '8-30D'
        when {{ days_expr }} <= 90 then '31-90D'
        when {{ days_expr }} <= 180 then '91-180D'
        when {{ days_expr }} <= 365 then '181D-1Y'
        else '>1Y'
    end
{%- endmacro %}


{#- HQLA 分级：证券类型 + 评级决定等级 -#}
{% macro hqla_level(security_type_expr, rating_expr) -%}
    case
        when {{ security_type_expr }} = 'TREASURY' then 'LEVEL_1'
        when {{ security_type_expr }} in ('AGENCY_DEBT', 'MBS') then 'LEVEL_2A'
        when {{ security_type_expr }} = 'CORP_BOND' and {{ rating_expr }} in ('AAA', 'AA', 'A') then 'LEVEL_2B'
        when {{ security_type_expr }} = 'EQUITY' and {{ rating_expr }} in ('AAA', 'AA', 'A') then 'LEVEL_2B'
        else 'NON_HQLA'
    end
{%- endmacro %}


{#- HQLA 折扣率：与上表分级一一对应 -#}
{% macro hqla_haircut(hqla_level_expr) -%}
    case
        when {{ hqla_level_expr }} = 'LEVEL_1' then 0.0000
        when {{ hqla_level_expr }} = 'LEVEL_2A' then 0.1500
        when {{ hqla_level_expr }} = 'LEVEL_2B' then 0.5000
        else 1.0000
    end
{%- endmacro %}


{#- 客户细分：把源系统的客户类型原始值归一到报送口径 -#}
{% macro customer_segment(customer_type_expr) -%}
    case
        when {{ customer_type_expr }} = 'IND' then 'RETAIL'
        when {{ customer_type_expr }} = 'CORP' then 'CORPORATE'
        when {{ customer_type_expr }} = 'FI' then 'FINANCIAL'
        when {{ customer_type_expr }} = 'GOV' then 'SOVEREIGN'
        else 'OTHER'
    end
{%- endmacro %}


{#- 存款产品类别：源系统产品类型 → FR 2052a 的产品口径 -#}
{% macro deposit_product_category(deposit_type_expr) -%}
    case
        when {{ deposit_type_expr }} = 'CHK' then 'DEMAND'
        when {{ deposit_type_expr }} = 'SAV' then 'SAVINGS'
        when {{ deposit_type_expr }} = 'MMDA' then 'SAVINGS'
        when {{ deposit_type_expr }} = 'CD' then 'CD'
        when {{ deposit_type_expr }} = 'TIME' then 'TIME'
        else 'OTHER'
    end
{%- endmacro %}
