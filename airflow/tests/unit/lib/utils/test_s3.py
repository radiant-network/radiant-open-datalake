from dags.lib.utils.s3 import load_file


def test_load_file_uploads_file_to_s3(s3_hook):
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"

    load_file(s3_hook, s3_bucket, dest_s3_key, local_file_name)

    s3_hook.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)

    # Since md5_hash is not provided, load_string should not be called
    s3_hook.load_string.assert_not_called()


def test_load_file_uploads_md5_file_when_md5_hash_provided(s3_hook):
    s3_bucket = "bucket"
    dest_s3_key = "key"
    local_file_name = "file.txt"
    md5_hash = "abc123"

    from dags.lib.utils.s3 import load_file

    load_file(s3_hook, s3_bucket, dest_s3_key, local_file_name, md5_hash=md5_hash)

    s3_hook.load_file.assert_called_once_with(local_file_name, dest_s3_key, s3_bucket, replace=True)
    s3_hook.load_string.assert_called_once_with(md5_hash, f"{dest_s3_key}.md5", s3_bucket, replace=True)
