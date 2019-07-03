#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path
import string
import argparse
import logging

logger = logging.getLogger(__name__)

#
# This script tests the data-validator command line parsing and handling of different errors.
#

validator_fail = "DATA_VALIDATOR_STATUS=FAIL"
validator_pass = "DATA_VALIDATOR_STATUS=PASS"

tests = [
    {
        "label": "Bad config path local",
        "args": { "config": "FileNotFound.yaml", "vars":"DATA_DIR=$DATA_DIR" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad local report.json path",
        "args": { "config": "$YAML_DIR/simple.yaml", "jsonReport":"very/bad/path.json", "vars":"DATA_DIR=$DATA_DIR,COL=id" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad hdfs report.json path",
        "args": { "config": "$YAML_DIR/simple.yaml", "jsonReport":"hdfs://badnn/very/bad/path.json", "vars":"DATA_DIR=$DATA_DIR,COL=id" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad local report.html path",
        "args": { "config": "$YAML_DIR/simple.yaml", "jsonReport":"very/bad/path.html", "vars":"DATA_DIR=$DATA_DIR,COL=id" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad hdfs report.html path",
        "args": { "config": "$YAML_DIR/simple.yaml", "htmlReport":"hdfs://badnn/very/bad/path.html", "vars":"DATA_DIR=$DATA_DIR,COL=id" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad cli option",
        "args": { "config": "$YAML_DIR/simple.yaml", "htmlReport":"hdfs://badnn/very/bad/path.html", "badOption":"" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad (missing) variable in config",
        "args": { "config": "$YAML_DIR/simple.yaml", "htmlReport":"hdfs://badnn/very/bad/path.html" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad YAML config",
        "args": { "config": "$YAML_DIR/bad.yaml" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "No problems",
        "args": { "config": "$YAML_DIR/simple.yaml", "vars":"DATA_DIR=$DATA_DIR,COL=id" },
        "returncode":0,
        "stdout_last": validator_pass
    },
    {
        "label": "nullCheck failures",
        "args": { "config": "$YAML_DIR/nullcheck.yaml", "vars":"DATA_DIR=$DATA_DIR" },
        "returncode":255,
        "stdout_last": validator_fail
    },
    {
        "label": "exit error on fail False (with nullCheck failures)",
        "args": { "config": "$YAML_DIR/nullcheck.yaml", "vars":"DATA_DIR=$DATA_DIR", "exitErrorOnFail":"false" },
        "returncode":0,
        "stdout_last": validator_fail
    },
    {
        "label": "Bad column",
        "args": { "config": "$YAML_DIR/simple.yaml", "vars":"DATA_DIR=$DATA_DIR,COL=BadColumn" },
        "returncode":255,
        "stdout_last": validator_fail
    }
]

def find_root():
    # src/test/resources/scripts/test_validator.py
    me = Path(sys.argv[0])
    root = (Path.cwd() / me.parent / ".." / ".." / ".." / ".." ).resolve()
    logger.debug("Root:{}".format(root))
    return root
    

def find_jar(root):
    # src/test/resources/scripts/test_validator.py
    jar_dir = root / "target" / "scala-2.11"

    logger.debug("jar_dir:{}".format(jar_dir))
    d = None
    for d in list(jar_dir.glob("data-validator-assembly*.jar")):
        ret = d
    if not d:
        logger.error("Could not find data-validator jar, try running `sbt assembly`")
        sys.exit(1)
    return  d

def sub_var(v,d):
    ret = string.Template(v).substitute(d)
    logger.debug("Input:'{}' d:{}".format(v,d))
    logger.debug("output:'{}'".format(ret))
    return ret

def last_nonblank(c):
    lines = [ (x.decode("utf-8")).strip() for x in c if len(x.strip()) > 0 ]
    lines.reverse()
    for l in lines:
        if len(l) > 0:
            logger.debug("Last Line:{}".format(l))
            return l
    return None

def run_test(conf,yaml_dir, data_dir, jar):
    tvars = { "YAML_DIR": yaml_dir, "DATA_DIR": data_dir }
    args = " ".join([ "--{} {}".format(k,sub_var(v,tvars)) for k,v in conf['args'].items()])
    cmd = "spark-submit --class com.tgt.edabi.dse.data_validator.Main --master local {} {}".format(jar, args)
    logger.debug("CMD:{}".format(cmd))

    result = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT, close_fds=True, shell=True)
    result.wait()

    stdout_lines = [ l for l in result.stdout]
    last_line = last_nonblank(stdout_lines)

    ret = False
    if result.returncode != conf['returncode']:
        ret = True
        logger.debug("Unexpected Return code returncode:{0:d} expected:{1:d}".format(result.returncode, conf['returncode']))
        logger.debug("stdout:{}".format(stdout_lines))

    if 'stdout_last' in conf and conf['stdout_last'] is not None:
        logger.debug("last_line:{} conf_expected:{}".format(last_line, conf['stdout_last']))
        if conf['stdout_last'] != last_line:
            ret = True
            logger.debug("Unexpected expected:{}".format(conf['stdout_last'], last_line))
            if logger.level == logging.DEBUG:
                for l in stdout_lines:
                    logger.debug("STDOUT:{}".format(l))
    else:
        logger.debug("No expected output!")

    return ret
    

def status(f):
    if f:
        return "FAIL"
    else:
        return "PASS"

def config_logging(log_level):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(log_level))
    logging.basicConfig(level=numeric_level, format="%(levelname)s - %(message)s")
    logger.info("Setting log_level to {}".format(log_level))
    
# main
arg_parser = argparse.ArgumentParser(description = "Test data-validator's Main.")
arg_parser.add_argument("--log", choices=[ "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
arg_parser.add_argument("-t", dest="tests", default=[], help="Run individual tests", type=int, action="append") 

args = arg_parser.parse_args(sys.argv[1:])

config_logging(args.log)

root = find_root()
jar = find_jar(root)
yaml_dir = root / "src/test/resources/conf"
data_dir = root / "src/test/resources/data"

logger.debug("JAR:{}".format(jar))
pass_cnt = 0
fail_cnt = 0

run_tests = range(len(tests))

if len(args.tests) > 0:
    run_tests = args.tests

for i in run_tests:
    t = tests[i]
    r = run_test(t,yaml_dir, data_dir, jar)
    logger.info("Test[{:>2}]:{:<50} {}".format(i,t['label'],status(r)))
    if r:
        fail_cnt += 1
    else:
        pass_cnt += 1

logger.info("FAILED:{:>3}".format(fail_cnt))
logger.info("PASSED:{:>3}".format(pass_cnt))

if fail_cnt > 0:
    logger.error("TEST FAILED!")
    logger.error("Run again with '--log DEBUG' for more info about failures.")
    sys.exit(1)
else:
    logger.info("TEST PASSED!")
