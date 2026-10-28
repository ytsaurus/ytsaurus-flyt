cd "$SERVICE_DIR"
export PYTHONPATH="$PYTHONPATH:$SERVICE_DIR"

# Credentials from secure_vault (YT injects YT_SECURE_VAULT_<KEY> for each vault key)
if command -v compgen >/dev/null 2>&1; then
  for _flyt_var in $(compgen -e | grep '^YT_SECURE_VAULT_' || true); do
    _flyt_key="${{_flyt_var#YT_SECURE_VAULT_}}"
    _flyt_val="$(printenv "${{_flyt_var}}")"
    export "${{_flyt_key}}=${{_flyt_val}}"
  done
fi
unset _flyt_var _flyt_key _flyt_val 2>/dev/null || true

export SERVICE_NAME="{service_name}"
