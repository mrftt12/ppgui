import os
import shutil
local_staging_dir='tmp'
pkg_name = 'powerflow'

pkg_src = 'src'
print(f"Creating zip -> {local_staging_dir}/{pkg_name}")
if os.path.isdir(pkg_src):
    shutil.make_archive(
        f"{local_staging_dir}/{pkg_name}", "zip", pkg_src
    )
