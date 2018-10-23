# -*- coding: utf-8 -*-
import subprocess
import json
import logging

logger = logging.getLogger('app')


def run_command(cmd):
    err = ""
    out = ""
    exitcode = ""
    try:
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        exitcode = proc.returncode
    except Exception as e:
        err = 'run_command error{}'.format(e)
        exitcode = '-10000'
    if exitcode == 0:  # 运行成功
        return exitcode, out + err
    else:
        return exitcode, out + err


class CeleryUtil(object):

    def __init__(self, base_dir='/Users/fanchun/Documents/web/INTProject/', proj='CYHProject'):
        self.proj = proj
        self.prefix_str = 'cd {}&&'.format(base_dir)

    def get_active_task(self):
        return self.inspect('active')

    def get_reserved_task(self):
        return self.inspect('reserved')

    def get_revoked_task(self):
        return self.inspect('revoked')

    def inspect(self, type):
        cmd = self.prefix_str + 'celery -A {} inspect {}'.format(self.proj, type)
        code, out = run_command(cmd)
        logger.info(cmd)
        logger.info(out)
        if isinstance(out, bytes):
            out = out.decode('utf8')
        replace_dic = {"\'": '\"', 'None': "null", 'True': 'true', 'False': 'false'}
        for k, v in replace_dic.items():
            out = out.replace(k, v)
        str_list = out.split('\n    * ')
        if not len(str_list) > 1:
            return []
        task_str_list = str_list[1:]
        tasks = [CeleryTask(task_str) for task_str in task_str_list]
        return tasks


class CeleryTask(object):
    task_id = ''
    info_dic = {}

    def __init__(self, info_str=''):
        if len(info_str) > 0:
            info_str = info_str.strip('\n')
            info_str = info_str.replace(': u\"',': \"').replace('{u\"','{\"').replace(', u\"',', \"')
            self.info_dic = json.loads(info_str)

        self.task_id = self.info_dic.get('id', '')


if __name__ == '__main__':
    CeleryUtil('/Users/fanchun/Documents/web/INTProject/', 'INTProject').get_active_task()
