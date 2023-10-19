import subprocess, os
import textwrap

ABT_PATH = "/home/tomoya-s/work/github/argobots/install"
MYLIB_PATH = "/home/tomoya-s/work/pthabt/newlib"

def get_wtperf_cmd(op, n_th, cache_size, db_path):
    key_size = 32
    val_size = 512
    num = 250 * 1000 * 1000
    duration = 120
    
    if op == "set":
        create = "true"
    else:
        create = "false"
        
    conf_str = textwrap.dedent('''
conn_config="cache_size={cache_size},direct_io=(data,checkpoint),eviction=(threads_min=12,threads_max=12),eviction_target=60,session_max={session_max}"
table_config="type=file,leaf_page_max=4k,internal_page_max=4k,checksum=on"
icount={num}
key_sz={key_size}
value_sz={val_size}
report_interval=1
run_time={duration}
populate_threads=1
verbose=1
create={create}
threads=((count={n_th},reads=1))
warmup=30
''').format(session_max=n_th, create=create, n_th=n_th, num=num, key_size=key_size, val_size=val_size, duration=duration, cache_size=cache_size)

    cfg_filename = "wtperf.cfg"
    with open(cfg_filename, "w") as cfg_file:
        cfg_file.write(conf_str)

    wtperf_path = "./bench/wtperf/wtperf"
    cmd = wtperf_path + " -O " + cfg_filename + " -h " + db_path
    return cmd


def run(mode, op, n_core, n_th, cache_size):
    if mode == "abt":
        db_path = "/home/tomoya-s/mountpoint/tomoya-s/wt_abt250m"
    else:
        db_path = "/home/tomoya-s/mountpoint/tomoya-s/wt_native250m"
    
    if op == "set":
        print("We are modifying database {}. Are you Sure? (Y/N)".format(db_path))
        x = input()
        assert x == "y"

    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_TH={}".format(MYLIB_PATH, ABT_PATH, n_core)
        process = subprocess.run(mylib_build_cmd.split())
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib"
        my_env["ABT_THREAD_STACKSIZE"] = "65536"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
        cmd = get_wtperf_cmd(op, n_th, cache_size, db_path)
    else:
        cmd = "taskset -c 0-{} ".format(n_core-1) + get_wtperf_cmd(op, n_th, cache_size, db_path)
        #cmd = get_wtperf_cmd(mode, op, n_th, cache_size, db_path)

    print(cmd)
    process = subprocess.run(cmd.split(), env=my_env)


#run("abt", "set", 1, 1, 1024*1024)
#run("natve", "set", 1, 1, 1024*1024)

run("native", "get", 8, 256, "12G")
#run("abt", "get", 8, 256, "12G")

# for n_core in [8,4,2,1]:
#     for n_pth in [256,128,64,32,16]:#[16,32,64,128,256]:
# #        run("abt", "get", n_core, n_pth, "12G")
#         run("native", "get", n_core, n_pth, "12G")
    
# for n_core in [1,2,4,8]:
#     for n_pth in [8,16,32,64,128]:
#         run("native", "get", n_core, n_pth, "12G")
