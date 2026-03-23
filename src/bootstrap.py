"""
Bootstrap: install missing pip packages on the Spark driver AND every executor.
Call install_on_driver() before any imports that need these packages.
Call broadcast_install(spark) once SparkSession exists to run the same
pip install on each executor worker before UDF tasks start.
"""

REQUIRED_PACKAGES = [
    "accelerate==0.34.2",
    "transformers==4.46.3",
    "docling",
    "ibm-watsonx-ai",
    "openai",
    "opensearch-py",
]


def install_on_driver(packages=None, quiet=False):
    """Install packages via pip on the driver process."""
    import sys
    import subprocess
    pkgs = packages or REQUIRED_PACKAGES
    print(f"[BOOTSTRAP] Installing on driver: {pkgs}", flush=True)
    cmd = [sys.executable, "-m", "pip", "install"] + pkgs
    if quiet:
        cmd.append("-q")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[BOOTSTRAP] pip driver install stderr:\n{result.stderr}", flush=True)
        raise RuntimeError(f"pip install failed on driver: {result.stderr[-500:]}")
    print("[BOOTSTRAP] Driver install OK", flush=True)


def broadcast_install(spark, packages=None, quiet=False):
    """
    Run pip install on every executor by submitting a dummy RDD job.
    Must be called after SparkSession is created.
    """
    import sys
    pkgs = packages or REQUIRED_PACKAGES

    def _worker_install(_):
        import subprocess, sys as _sys
        cmd = [_sys.executable, "-m", "pip", "install"] + pkgs
        if quiet:
            cmd.append("-q")
        r = subprocess.run(cmd, capture_output=True, text=True)
        return r.returncode, r.stderr[-300:] if r.returncode != 0 else "OK"

    num_executors = int(spark.conf.get("spark.executor.instances", "1"))
    print(f"[BOOTSTRAP] Installing on {num_executors} executor(s): {pkgs}", flush=True)
    results = (
        spark.sparkContext
        .parallelize(range(num_executors), num_executors)
        .map(_worker_install)
        .collect()
    )
    failures = [(i, err) for i, (rc, err) in enumerate(results) if rc != 0]
    if failures:
        raise RuntimeError(f"pip install failed on executor(s): {failures}")
    print("[BOOTSTRAP] Executor install OK", flush=True)
