
from os import path, makedirs, OSError
from glob import glob
import shutil

from subprocess import Popen, PIPE

from ..job import Job

from ..handler import Handler

import ..util

from ..error import RegistrationError, \
                  FinalizationError, \
                  PostprocessingError, \
                  HostUnavailableError, \
                  StartingError, \
                  PreparationError, \
                  GitExecError


class SSHGitHandler(Handler):

    def prepare(self, daemon):
        if not path.isdir(self.jobdir):     
            try: 
                makedirs(self.jobdir)
                print "[jodaepy] Handler: Created job directory %s" % self.jobdir
            except OSError:
                raise PreparationError("Jobdir '%s' not found" % self.jobdir)

        if not path.isdir(self.jobarchive): 
            try: 
                makedirs(self.jobarchive)
                print "[jodaepy] Handler: Created job archive %s" % self.jobarchive
            except OSError:
                raise PreparationError("Jobarchive '%s' not found" % self.jobarchive)

        if not path.isdir(self.joblogdir):  
            try:
                makedirs(self.joblogdir)
                print "[jodaepy] Handler: Created joblog director %s" % self.joblogdir
            except OSError:
                raise PreparationError("Joblog directory '%s' not found" % self.joblogdir)

        return True


    def __init__( self,
                  hosts,
                  jobdir,
                  jobarchive,
                  joblogdir,
                  rundirs,
                  scriptdirs,
                  jobslots
                ):
        
        self.hosts  = [hosts] if type(hosts) == str else hosts
        self.jobdir = jobdir
        self.jobarchive = jobarchive
        self.joblogdir = joblogdir


        if type(rundirs) == str:
            self.rundirs = {}
            for host in self.hosts:
                self.rundirs[host] = rundirs
        else:
            self.rundirs = rundirs


        if type(scriptdirs) == str:
            self.scriptdirs = {}
            for host in self.hosts:
                self.scriptdirs[host] = scriptdirs
        else:
            self.scriptdirs = scriptdirs


        if type(jobslots) == int:
            self.jobslots = {}
            for host in self.hosts:
                self.jobslots[host] = jobslots
        else:
            self.jobslots = jobslots


        self.scripts_old = None


    def finalize_job(self, daemon, job):
       job.write_log(self.joblogdir)
       # commit the outfiles of this job
       try:
           util.git_add('.', self.joblogdir)
           if job.outfiles:
               util.git_add(job.outfiles, self.rundirs[job.running_host])
           util.git_commit(self.rundirs[job.running_host], "new result files %s" %
                                                      job.outfiles )
           util.git_pull(self.rundirs[job.running_host])
           util.git_push(self.rundirs[job.running_host])
       except GitExecError as error:
           raise FinalizationError(str(error))
        

    def check_host(self, host):
        cmd = ["ssh", "-o", "BatchMode=yes", host, "true"]
        process = Popen(cmd)
        process.communicate()
        return process.returncode == 0


    def start_job(self, daemon, job, host):

        rcmd = ["ssh", "-o", "BatchMode=yes", host]
        rcmd.append("PATH=%s:$PATH; cd %s; %s" % (self.scriptdirs[host], self.rundirs[host], job.cmd))
        try:
            process = Popen(rcmd, stdout=PIPE, stderr=PIPE)
            job.set_process(process, host)
        except OSError as error:
            raise StartingError(str(error))


    def update_workspace( self, daemon ):
        try:
            util.git_pull(self.jobdir)
        except GitExecError as error:
            raise RegistrationError("Could not update workspace: " + str(error))

        jobfiles = [ path.basename(s) for s in glob(path.join(self.jobdir, "*.job")) ]

        return jobfiles


    def update_scripts( self, daemon ):
        # broken for more general situations
        sdir = self.scriptdirs[self.hosts[0]]
        scripts = [ path.basename(s) for s in glob(path.join(sdir, "*")) ]
        if self.scripts_old: 
            dscripts = [ s for s in scripts if s not in self.scripts_old ]
            self.scripts_old = scripts
            return dscripts
        else:
            self.scripts_old = scripts
            return scripts



    def register_jobs( self, daemon ):
        # Make a git pull to obtain new job files
        jobfiles = [ path.basename(j) for j in glob(path.join(self.jobdir, "*.job")) ]

        new_jobs = []
        def JOB(*args, **kwargs): new_jobs.append(Job(*args, **kwargs))

        # Try to load the jobfiles
        for jobfile in jobfiles:
            jobpath = path.join(self.jobdir, jobfile)
            try:
                execfile(jobpath)
            except Exception as error:
                move_to_dir(jobpath, self.jobarchive)
                msg = "Error executing jobfile %s. " % jobfile +\
                      "Moved it to job archive %s. " % self.jobarchive +\
                      "Original error message:\n" + str(error)
                try:
                    util.git_add(jobfiles, self.jobarchive)
                    util.git_add(jobfiles, self.jobdir)
                    #print "git add %s" % archived_files
                    util.git_commit(self.jobarchive, "registered jobs %s" % 
                                                [job.id for job in new_jobs] )
                    #print "git commit"
                    util.git_pull(self.jobarchive)
                    util.git_push(self.jobarchive)
                except GitExecError as error:
                    raise RegistrationError(msg + "\n" + str(error))
                raise RegistrationError(msg)
        

        # So it worked for every file. Move them to archive and push
        for jobfile in jobfiles:
            jobpath = path.join(self.jobdir, jobfile)
            move_to_dir(jobpath, self.jobarchive)

        # BUG: if a jobfile in jobarchive is overwritten with a content-identical
        # file, git commit will fail
        if jobfiles:
            try:
                util.git_add(jobfiles, self.jobarchive)
                util.git_add(jobfiles, self.jobdir)
                util.git_commit(self.jobarchive, "registered jobs %s" % 
                                            [job.id for job in new_jobs] )
                util.git_pull(self.jobarchive)
                util.git_push(self.jobarchive)

            except GitExecError as error:
                raise RegistrationError(str(error))

        return new_jobs


