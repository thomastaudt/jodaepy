

import os
import subprocess as sp

from error import GitExecError

def execute(git_cmd, path):
    pipe = sp.Popen(git_cmd, cwd = path, stdout = sp.PIPE, stderr = sp.PIPE)
    outdata, errdata = pipe.communicate()
    if pipe.returncode != 0: raise GitExecError(errdata)


def pull(path):
    execute(['git', 'pull', '--no-edit'], path)


def push(path):
    execute(['git', 'push'], path)


def add(files, path):
    git_cmd = ['git', 'add']
    git_cmd.extend(files)
    execute(git_cmd, path)


def commit(path, message):
    git_cmd = ['git', 'commit', '--no-edit', '-m', message]
    execute(git_cmd, path)
