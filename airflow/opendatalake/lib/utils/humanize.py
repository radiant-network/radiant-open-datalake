def bytes_to_human_readable(byte_size: int) -> str:
    """
    Convert a byte size into a human-readable string (Bytes, KB, MB, GB, TB, PB).
    """
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB"]
    size = float(byte_size)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
