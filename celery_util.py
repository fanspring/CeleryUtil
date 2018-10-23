# -*- coding: utf-8 -*-
from INTProject.celery import app


class CeleryUtil(object):

    def __init__(self):
        self.inspect = app.control.inspect()
        self.control = app.control

    def inspect_cmd(self, cmd):
        dic = getattr(self.inspect, cmd)()
        tasks = []
        for k in dic.keys():
            tasks += dic[k]
        return tasks

    def inspect_task_ids(self, cmd):
        tasks = self.inspect_cmd(cmd)
        return [task['id'] for task in tasks]

    def active_task_ids(self):
        """
        列出所有在运行的任务id
        :return: 任务id列表
        """
        return self.inspect_task_ids('active')

    def reserved_task_ids(self):
        """
        列出所有等待中的任务id
        :return: 任务id列表
        """
        return self.inspect_task_ids('reserved')

    def revoked_task_ids(self):
        """
        列出所有取消的任务id
        :return: 任务id列表
        """
        return self.inspect_cmd('revoked')

    def revoke(self, task_id):
        """
        取消任务
        :param task_id: 任务id
        """
        self.control.revoke(task_id)

    def terminate(self, task_id):
        """
        终止任务
        :param task_id: 任务id
        """
        self.control.terminate(task_id)

    def purge(self):
        """
        清空队列
        """
        self.control.purge()
