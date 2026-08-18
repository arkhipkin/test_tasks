{% macro dw_modified_at() -%}
current_timestamp()::timestamp_ntz
{%- endmacro %}


{% macro seed_value_to_boolean(expr) -%}
case
    when upper(trim(cast({{ expr }} as varchar))) in ('TRUE', 'T', '1', 'Y', 'YES') then true
    when upper(trim(cast({{ expr }} as varchar))) in ('FALSE', 'F', '0', 'N', 'NO') then false
    else null
end
{%- endmacro %}
