# sandbox_unpack: unpack runtime-*.squashfs into $ROOT_DIR (from 00_set_essentials.sh).
UNSQUASHFS=""
if command -v unsquashfs >/dev/null 2>&1; then
    UNSQUASHFS=$(command -v unsquashfs)
elif [ -x "$ROOT_DIR/flyt_unsquashfs" ]; then
    UNSQUASHFS="$ROOT_DIR/flyt_unsquashfs"
elif [ -f "$ROOT_DIR/flyt_unsquashfs" ]; then
    chmod +x "$ROOT_DIR/flyt_unsquashfs"
    UNSQUASHFS="$ROOT_DIR/flyt_unsquashfs"
fi
if [ -z "$UNSQUASHFS" ]; then
    echo "No unsquashfs (install squashfs-tools in the exec image, or let flyt upload flyt_unsquashfs for sandbox_unpack)" 1>&2
    exit 1
fi
_sq=""
for _cand in "$ROOT_DIR"/runtime-*.squashfs; do
    if [ -f "$_cand" ]; then
        _sq="$_cand"
        break
    fi
done
if [ -z "$_sq" ]; then
    echo "No runtime-*.squashfs found in $ROOT_DIR (expected flyt runtime layer file)" 1>&2
    exit 1
fi
echo "Unpacking $_sq into $ROOT_DIR using $UNSQUASHFS..." 1>&2
"$UNSQUASHFS" -f -d "$ROOT_DIR" "$_sq"
