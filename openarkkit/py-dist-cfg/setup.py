from distutils.core import setup

setup(
    name="openark-kit",
    description="Common utilities for MySQL",
    author="Shlomi Noach",
    author_email="shlomi@code.openark.org",
    url="http://code.openark.org/forge/openark-kit",
    version="96",
    requires=["MySQLdb"],
    packages=[""],
    package_dir={"": "scripts"},
    scripts=[
        "scripts/oak-apply-ri",
        "scripts/oak-block-account",
        "scripts/oak-chunk-update",
        "scripts/oak-kill-slow-queries",
        "scripts/oak-modify-charset",
        "scripts/oak-online-alter-table",
        "scripts/oak-purge-master-logs",
        "scripts/oak-security-audit",
        "scripts/oak-show-limits",
        "scripts/oak-show-replication-status",
        ]
)
