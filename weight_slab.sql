-- macros/weight_slab.sql
-- Returns a human-readable weight slab label given min/max kg values

{% macro weight_slab_label(min_col, max_col) %}
    case
        when {{ max_col }} <= 0.5  then '0–0.5 kg'
        when {{ max_col }} <= 1.0  then '0.5–1 kg'
        when {{ max_col }} <= 2.0  then '1–2 kg'
        when {{ max_col }} <= 5.0  then '2–5 kg'
        when {{ max_col }} <= 10.0 then '5–10 kg'
        when {{ max_col }} <= 20.0 then '10–20 kg'
        when {{ max_col }} <= 50.0 then '20–50 kg'
        else '50+ kg'
    end
{% endmacro %}
