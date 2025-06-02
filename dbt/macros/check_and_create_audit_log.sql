{% macro check_and_create_audit_log() %}
  {% if execute %}
    CREATE TABLE IF NOT EXISTS audit_log (
      model TEXT,
      run_at TIMESTAMPTZ
    );
  {% endif %}
{% endmacro %}