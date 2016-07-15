


class Communicator:

    def prepare(self, daemon): pass

    def registration_failed(self, daemon, error): pass
    def finalization_failed(self, daemon, job, error): pass
    def starting_failed(self, daemon, job, error): pass
    def postprocessing_failed(self, daemon, error): pass

    def workspace_updated(self, deamon, msg): pass

    def jobs_registered(self, daemon, jobs): pass
    def jobs_started(self, daemon, job): pass
    def jobs_returned(self, daemon, jobs): pass
    def jobs_finalized(self, daemon, job): pass

    def job_updated(self, daemon, job, dpercs, doutfiles): pass

    def postprocessing_done(self, daemon, pp): pass

    def host_unavailable(self, daemon, job): pass

    def communicate(self, daemon): pass



class StreamCommunicator(Communicator):
    
    def __init__(self, stream):
        self.stream = stream

    def registration_failed(self, daemon, error):
        self.stream.write("[jodaepy] Registration Failed: ")
        self.stream.write(str(error))
        self.stream.write("\n")

    def finalization_failed(self, daemon, job, error):
        self.stream.write("[jodaepy] Finalization Failed (job %d): " % job.id)
        self.stream.write(str(error))
        self.stream.write("\n")

    def starting_failed(self, daemon, job, error):
        self.stream.write("[jodaepy] Could not start job (job %d): " % job.id)
        self.stream.write(str(error))
        self.stream.write("\n")

    def postprocessing_failed(self, daemon, error):
        self.stream.write("[jodaepy] Postprocessing failed:")
        self.stream.write(str(error))
        self.stream.write("\n")


    def workspace_updated(self, deamon, files):
        if files:
            self.stream.write("[jodaepy] New files  " + " ".join(files) + "\n")

    def scripts_updated(self, deamon, scripts):
        if scripts:
            self.stream.write("[jodaepy] New scripts  " + " ".join(scripts) + "\n")

    
    def jobs_registered(self, daemon, jobs):
        if jobs:
            self.stream.write("[jodaepy] Registered jobs\n")
            for job in jobs:
                self.stream.write("          %5d '%s'\n" % (job.id, job.title) )


    def jobs_started(self, daemon, jobs):
        if jobs:
            self.stream.write("[jodaepy] Started jobs\n")
            for job in jobs:
                self.stream.write("          %5d, on '%s'\n" % (job.id, job.running_host) )

    def jobs_returned(self, daemon, jobs): pass

    def jobs_finalized(self, daemon, jobs):
        if jobs:
            self.stream.write("[jodaepy] Finalized jobs\n")
            for job in jobs:
                self.stream.write("          %5d (%s), %s -- %s\n" % \
                          (job.id, "fail" if job.failed() else "sucess", job.stime, job.ftime) )


    def postprocessing_done(self, daemon, pp):
        if pp:
            self.stream.write("[jodaepy] Postprocessing done\n")

    def host_unavailable(self, daemon, job):
        self.stream.write("[jodaepy] Expected hosts %s for job %d are unavailable. Dropped this job\n" % (str(job.hosts), job.id) )

    def communicate(self, daemon):
        pass


    def job_updated(self, daemon, job, dpercs, doutfiles):
        perc_old, perc = dpercs
        outfiles_old, outfiles= doutfiles

        if perc_old < 25 and perc > 25 or \
           perc_old < 50 and perc > 50 or \
           perc_old < 75 and perc > 75:
            self.stream.write("[jodaepy] Job %d reports: %d%% completed\n" % (job.id, perc) )
