# sandbox_unpack: unpack the runtime layer(s) into $ROOT_DIR (from 00_set_essentials.sh).
# unsquashfs: the exec image's binary if present, else the one flyt shipped (unsquashfs_path).
_FLYT_UNSQUASHFS_NAME="{unsquashfs_basename}"
UNSQUASHFS=""
if command -v unsquashfs >/dev/null 2>&1; then
    UNSQUASHFS=$(command -v unsquashfs)
elif [ -n "$_FLYT_UNSQUASHFS_NAME" ] && [ -f "$ROOT_DIR/$_FLYT_UNSQUASHFS_NAME" ]; then
    chmod +x "$ROOT_DIR/$_FLYT_UNSQUASHFS_NAME" 2>/dev/null || true
    UNSQUASHFS="$ROOT_DIR/$_FLYT_UNSQUASHFS_NAME"
fi
if [ -z "$UNSQUASHFS" ]; then
    echo "No unsquashfs available. Install squashfs-tools in the exec image, or set unsquashfs_path in the profile (build it with 'flyt build unsquashfs --upload <path>')." 1>&2
    exit 1
fi
# Explicit ordered layer list (templated); later layers overlay earlier ones.
# Empty -> fall back to the single flyt-built runtime-*.squashfs.
_FLYT_LAYERS="{squashfs_unpack_basenames}"
if [ -n "$_FLYT_LAYERS" ]; then
    for _name in $_FLYT_LAYERS; do
        _sq="$ROOT_DIR/$_name"
        if [ ! -f "$_sq" ]; then
            echo "Layer $_sq not found in sandbox" 1>&2
            exit 1
        fi
        echo "Unpacking $_sq into $ROOT_DIR using $UNSQUASHFS..." 1>&2
        "$UNSQUASHFS" -f -d "$ROOT_DIR" "$_sq"
    done
else
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
fi
