{# Same masking rules as pipeline/silver.py (EMAIL_RE, PHONE_RE). #}
{% macro mask_pii(col) -%}
regexp_replace(
    regexp_replace({{ col }}, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', '<EMAIL>', 'g'),
    '(\+84|0)[ .-]?[0-9]{2,3}[ .-]?[0-9]{3}[ .-]?[0-9]{3,4}', '<PHONE>', 'g')
{%- endmacro %}
