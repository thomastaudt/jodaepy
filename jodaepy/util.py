
import os
import shutil
import subprocess as sp

from error import GitExecError


def move_to_dir(source, target_dir):
    target = os.path.join(target_dir, os.path.basename(source))
    if os.path.exists(target): os.remove(target)
    shutil.move(source, target)


def git_execute(git_cmd, path):
    pipe = sp.Popen(git_cmd, cwd = path, stdout = sp.PIPE, stderr = sp.PIPE)
    outdata, errdata = pipe.communicate()
    if pipe.returncode != 0: raise GitExecError(errdata)


def git_pull(path):
    git_execute(['git', 'pull'], path)


def git_push(path):
    git_execute(['git', 'push'], path)


def git_add(files, path, option = None):
    git_cmd = ['git', 'add']
    if option: git_cmd.append(option)
    git_cmd.extend(files)
    git_execute(git_cmd, path)


def git_commit(path, message):
    git_cmd = ['git', 'commit', '-m', message]
    git_execute(git_cmd, path)

def git_commit_all(path, message):
    git_cmd = ['git', 'commit', '-a', '-m', message]
    git_execute(git_cmd, path)
