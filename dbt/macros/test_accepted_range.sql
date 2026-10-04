{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
-- Hand-rolled, not dbt_utils.accepted_range — this project has no dbt packages
-- installed (see dbt_project.yml / DECISIONS.md, Phase 8), so a one-off generic
-- test is simpler than adding a package dependency for a single check. NULLs
-- pass (are not flagged as failures) — several columns this test is applied to
-- (e.g. emissions_intensity) are legitimately NULL for sellers with no basis to
-- estimate them, which is not a range violation.
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
    {% if min_value is not none %} {{ column_name }} < {{ min_value }} {% endif %}
    {% if min_value is not none and max_value is not none %} OR {% endif %}
    {% if max_value is not none %} {{ column_name }} > {{ max_value }} {% endif %}
  )
{% endtest %}
