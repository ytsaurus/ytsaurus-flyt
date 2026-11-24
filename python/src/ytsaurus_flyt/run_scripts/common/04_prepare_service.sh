mkdir -p "$SERVICE_DIR"
# A job made only of the operation's files has no service wheel.
if [ -f service.whl ]; then
    unzip -qq service.whl -d "$SERVICE_DIR"
fi
