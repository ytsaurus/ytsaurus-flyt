cd "$SERVICE_DIR"
export PYTHONPATH="$PYTHONPATH:$SERVICE_DIR"

# Credentials from secure_vault (YT injects YT_SECURE_VAULT_<KEY> for each vault key)
for _flyt_var in ${{!YT_SECURE_VAULT_@}}; do
  export "${{_flyt_var#YT_SECURE_VAULT_}}=${{!_flyt_var}}"
done
unset _flyt_var

export SERVICE_NAME="{service_name}"
