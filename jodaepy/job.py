
# python standard modules
import time
import os
import fcntl
import re


class JobError(Exception):
    pass


class HostNotAvailableError(JobError):
    pass

class Job:
    def __init__( self, cmd, title="", 
                  descr="", id=-1, 
                  project=None, 
                  hosts=None, 
                  tags=[],
                  script_backup=False,
                  priority=0
                ):

        self.cmd    = cmd
        self.project = project
        self.title  = title
        self.descr  = descr
        self.id  = id
        self.hosts  = hosts
        self.tags   = []

        self.stdout = []
        self.stderr = []
        self.priority = priority

        self.outfiles = []

        self.stime  = None
        self.ftime = None

        self.perc = 0

        self.process = None
        self.retcode = None

        self.script = cmd.split()[0] if script_backup else None

    def set_id( self, id ):
        self.id = id


    def read_stdout(self):
        if not self.process: return ""
        fd = self.process.stdout.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
            return self.process.stdout.read()
        except:
            return "" 

    def read_stderr(self):
        if not self.process: return ""
        fd = self.process.stderr.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
            return self.process.stderr.read()
        except:
            return "" 


    def set_process(self, process, host):
        self.process = process
        self.stime = time.strftime("%H:%M:%S, %d.%m.%Y")
        self.stderr = []
        self.stdout = []
        self.outfiles = []

        self.running_host = host
        self.retcode = None
        self.ftime = None


    def started(self):
        return True if self.stime else False

    def returned(self):
        if self.retcode: return True

        self.retcode = self.process.poll()

        if self.retcode == None: return False

        self.ftime = time.strftime("%H:%M:%S, %d.%m.%Y")

        self.noerr = True if self.retcode == 0 else False

        return True

    def failed(self):
        if self.retcode == 0 or self.retcode == None:
            return False
        else:
            return True
        


    def update_status(self):

        outfiles = []
        info = None   # TODO

        outfiles_old = list(self.outfiles) # copy the list
        perc_old = self.perc

        stdout = self.read_stdout()
        stderr = self.read_stderr()

        if stdout:
            self.stdout.append(stdout)
            match = None
            for match in re.finditer(r"!perc:[0-9]+", stdout): pass
            if match: self.perc = int(match.group(0)[6:])

            for match in re.finditer(r"!file:\".*?\"", stdout):
                self.outfiles.append(match.group(0)[7:-1])

        if stderr:
            self.stderr.append(stderr)

        return (perc_old, self.perc), (outfiles_old, self.outfiles)



    def joblog_str(self):
        
        if not self.returned():
            raise JobError("Can't create joblog for unfinished job")

        content = ["!JOB FAILED!\n"] if self.failed() else []
        
        content.extend( [ "ID: %d" % self.id,
                          "TITLE: %s" % self.title,
                          "PROJECT: %s" % self.project if self.project else "",
                          "CMD: %s" % self.cmd,
                          "HOST: %s" % self.running_host,
                          #"RCMD: %s" % ' '.join(self.rcmd),
                          "TAGS: %s" % ' '.join(self.tags),
                          "START TIME: %s" % self.stime,
                          "FINISH TIME: %s" % self.ftime,
                          "FILES: %s" % ' '.join(self.outfiles),
                          "\n",
                          "DESCRIPTION:\n%s" % self.descr,
                          "\n",
                          "STDOUT:\n%s" % ''.join(self.stdout),
                          "\n",
                          "STDERR:\n%s" % ''.join(self.stderr),
                          "\n",
                          "SCRIPT:\n%s" % "TODO" # TODO
                        ] )
        return '\n'.join(content)

    def write_log(self, joblogdir):

        self.update_status()

        logpath = os.path.join(joblogdir, "%010d.jlog" % self.id)

        if self.returned():
            with open(logpath, "w") as log:
                log.write(self.joblog_str())



    def overview(self):

        if not self.started():
            status = "pending"
        elif not self.returned():
            status = "running"
        elif not self.failed():
            status = "finished"
        else:
            status = "failed"

        status += " (%d%%)" % self.perc

        content = []
        
        content.extend( [ "STATUS: %s" % status,
                          "ID: %d" % self.id,
                          "TITLE: %s" % self.title,
                          #"PROJECT: %s" % (self.project if self.project else "/"),
                          "CMD: %s" % self.cmd,
                          "HOST: %s" % self.running_host,
                          #"RCMD: %s" % ' '.join(self.rcmd),
                          #"TAGS: %s" % ' '.join(self.tags),
                          "START TIME: %s" % self.stime,
                          "FINISH TIME: %s" % self.ftime,
                          "FILES: %s" % ' '.join(self.outfiles),
                          "DESCRIPTION:\n%s" % self.descr,
                          "STDOUT:\n%s" % ''.join(self.stdout[-5:]),
                          "STDERR:\n%s" % ''.join(self.stderr[-5:]),
                        ] )
        return '\n'.join(content)
