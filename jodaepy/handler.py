

import os
import shutil
from glob import glob

#from asyncproc import Process
from subprocess import Popen, PIPE
import git
from job import Job

from error import RegistrationError, FinalizationError, PostprocessingError, HostUnavailableError, StartingError, PreparationError, GitExecError

#def cmd_to_rcmd(cmd, host, rundir, scriptdir):
    #rcmd = ["ssh", host]
    #rcmd.append("'PATH=%s:$PATH; cd %s; %s'" % (scriptdir, rundir, cmd))
    #return rcmd

def move_to_dir(source, target_dir):
    target = os.path.join(target_dir, os.path.basename(source))
    if os.path.exists(target): os.remove(target)
    shutil.move(source, target)


class Handler:
    def __init__(self, hosts, jobslots):
        self.hosts = hosts
        self.jobslots = jobslots

    def prepare(self, daemon):
        pass

    def update_workspace(self, daemon):
        pass

    def register_jobs(self, daemon):
        pass

    def start_job(self, daemon, job, host):
        pass

    def finalize_job(self, daemon, job):
        pass

    def postprocess(self, daemon):
        pass


class SSHGitHandler(Handler):

    def prepare(self, daemon):
        if not os.path.isdir(self.jobdir):     
            raise PreparationError("Jobdir '%s' not found" % self.jobdir)
        if not os.path.isdir(self.jobarchive): 
            raise PreparationError("Jobarchive '%s' not found" % self.jobarchive)
        if not os.path.isdir(self.joblogdir):  
            raise PreparationError("Joblogdir '%s' not found" % self.joblogdir)

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
           git.add('.', self.joblogdir)
           if job.outfiles:
               git.add(job.outfiles, self.rundirs[job.running_host])
           git.commit(self.rundirs[job.running_host], "new result files %s" %
                                                      job.outfiles )
           git.pull(self.rundirs[job.running_host])
           git.push(self.rundirs[job.running_host])
       except GitExecError as error:
           raise FinalizationError(str(error))
        

    def start_job(self, daemon, job, host):

        rcmd = ["ssh", host]
        rcmd.append("PATH=%s:$PATH; cd %s; %s" % (self.scriptdirs[host], self.rundirs[host], job.cmd))
        try:
            process = Popen(rcmd, stdout=PIPE, stderr=PIPE)
            job.set_process(process, host)
        except OSError as error:
            raise StartingError(str(error))


    def update_workspace( self, daemon ):
        try:
            git.pull(self.jobdir)
        except GitExecError as error:
            raise RegistrationError("Could not update workspace: " + str(error))

        jobfiles = [ os.path.basename(s) for s in glob(os.path.join(self.jobdir, "*.job")) ]

        return jobfiles

    def update_scripts( self, daemon ):
        # broken for more general situations
        sdir = self.scriptdirs[self.hosts[0]]
        scripts = [ os.path.basename(s) for s in glob(os.path.join(sdir, "*")) ]
        if self.scripts_old: 
            dscripts = [ s for s in scripts if s not in self.scripts_old ]
            self.scripts_old = scripts
            return dscripts
        else:
            self.scripts_old = scripts
            return scripts



    def register_jobs( self, daemon ):
        # Make a git pull to obtain new job files
        jobfiles = [ os.path.basename(j) for j in glob(os.path.join(self.jobdir, "*.job")) ]

        new_jobs = []
        def JOB(*args, **kwargs): new_jobs.append(Job(*args, **kwargs))

        # Try to load the jobfiles
        for jobfile in jobfiles:
            jobpath = os.path.join(self.jobdir, jobfile)
            try:
                execfile(jobpath)
            except Exception as error:
                move_to_dir(jobpath, self.jobarchive)
                msg = "Error executing jobfile %s. " % jobfile +\
                      "Moved it to job archive %s. " % self.jobarchive +\
                      "Original error message:\n" + str(error)
                try:
                    git.add(jobfiles, self.jobarchive)
                    git.add(jobfiles, self.jobdir)
                    #print "git add %s" % archived_files
                    git.commit(self.jobarchive, "registered jobs %s" % 
                                                [job.id for job in new_jobs] )
                    #print "git commit"
                    git.pull(self.jobarchive)
                    git.push(self.jobarchive)
                except GitExecError as error:
                    raise RegistrationError(msg + "\n" + str(error))
                raise RegistrationError(msg)
        

        # So it worked for every file. Move them to archive and push
        for jobfile in jobfiles:
            jobpath = os.path.join(self.jobdir, jobfile)
            move_to_dir(jobpath, self.jobarchive)

        # BUG: if a jobfile in jobarchive is overwritten with a content-identical
        # file, git commit will fail
        if jobfiles:
            try:
                git.add(jobfiles, self.jobarchive)
                git.add(jobfiles, self.jobdir)
                #print "git add %s" % archived_files
                git.commit(self.jobarchive, "registered jobs %s" % 
                                            [job.id for job in new_jobs] )
                #print "git commit"
                git.pull(self.jobarchive)
                git.push(self.jobarchive)
            except GitExecError as error:
                raise RegistrationError(str(error))
            #print "git push"

        return new_jobs


