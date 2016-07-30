
from datetime import datetime
from time import sleep

from error import RegistrationError, \
                  FinalizationError, \
                  PostprocessingError, \
                  HostUnavailableError, \
                  StartingError, \
                  PreparationError

#
# The daemon
#

class Daemon:
    def __init__( self,
                  handler,
                  communicators,
                  control_interval = 5,
                  handle_interval  = 15,
                  comm_interval    = 1,
                  initial_id       = 0
                ):

        self.handler = handler
        self.hosts   = self.handler.hosts
        self.jobslots = self.handler.jobslots
        self.communicators = communicators

        self.control_interval = control_interval
        self.handle_interval  = handle_interval
        self.comm_interval    = comm_interval

        self.control_counter = 0
        self.handle_counter  = 0
        self.comm_counter    = 0

        self.handle  = False
        self.control = False
        self.comm    = False


        self.pending_jobs  = []
        self.returned_jobs = []
        self.failed_jobs   = []
        self.finished_jobs = []

        self.next_id = initial_id

        self.running_jobs  = {} # one list for every host
        for host in self.hosts:
            self.running_jobs[host] = []


    def run(self):

        #
        # Prepare conditions for the handler and communicator
        #
        self.handler.prepare(self)

        print "[jodaepy] Handler of type '%s' prepared sucessfully" % self.handler.__class__.__name__

        for comm in self.communicators:
            comm.prepare(self)
            print "[jodaepy] Communicator of type '%s' prepared sucessfully" % comm.__class__.__name__

        #
        # Timed main loop
        #

        time = 0.
        dt0 = datetime.now()

        self.quit = False

        print "[jodaepy] Daemon now running..."

        self.handle  = True
        self.control = True
        self.comm    = True

        while not self.quit:

            #
            # Core-function
            #

            try:

                self.core()

            except KeyboardInterrupt as error:
                for comm in self.communicators:
                    comm.close()
                raise error




            #
            # Check if to handle
            #

            counter = int( time / self.handle_interval )
            if counter > self.handle_counter: 
                self.handle_counter = counter
                self.handle = True
            else:
                self.handle = False


            #
            # Check if to control
            #

            counter = int( time / self.control_interval )
            if counter > self.control_counter:
                self.control_counter = counter
                self.control = True
            else:
                self.control = False


            #
            # Check if to communicate
            #

            counter = int( time / self.comm_interval )
            if counter > self.comm_counter:
                self.comm_counter = counter
                self.comm = True
            else:
                self.comm = False

            
            #
            # Sleep till next iteration
            #

            time = (datetime.now() - dt0).total_seconds()

            sleeptime = int(time) + 1 - time
            sleep(int(time) + 1 - time)


    def core(self):

        #
        # Handle: Register new jobs, finish returned jobs
        #

        if self.handle:

            #
            # Handling new jobs
            #


            try:
                
                files = self.handler.update_workspace(self)

                for comm in self.communicators:
                    comm.workspace_updated(self, files)

                scripts = self.handler.update_scripts(self)

                for comm in self.communicators:
                    comm.scripts_updated(self, scripts)


                new = self.handler.register_jobs(self)
            
                self.pending_jobs.extend(new)

                for job in new: self.register(job)
                self.pending_jobs.sort(key = lambda job: - job.priority)


                for comm in self.communicators:
                    comm.jobs_registered(self, new)

            except RegistrationError as error:
                for comm in self.communicators:
                    comm.registration_failed(self, error)


            #
            # Finish jobs
            #

            

            finalized = []
            for job in reversed(self.returned_jobs):
                try:
                    self.handler.finalize_job(self, job)

                    if job.failed(): self.failed_jobs.append(job)
                    else:            self.finished_jobs.append(job)

                    self.returned_jobs.remove(job)
                    finalized.append(job)


                except FinalizationError as error:
                    self.returned_jobs.remove(job)
                    self.failed_jobs.append(job)
                    for comm in self.communicators:
                        comm.finalization_failed(self, job, error)


            for comm in self.communicators:
                comm.jobs_finalized(self, finalized)


            #
            # Postprocessing (upload result files or similar stuff)
            #

            try:
                pp = self.handler.postprocess(self)

                for comm in self.communicators:
                    comm.postprocessing_done(self, pp)

            except PostprocessingError as error:
                for comm in self.communicators:
                    comm.postprocessing_failed(self, job, error)



        #
        # Control: Check for returned jobs, start pending jobs
        #
            
        if self.control:

            #
            # Check if running jobs returned
            #
            
            returned = []

            # TODO: make this code prettier
            for host in self.hosts:
                _returned = [ job for job in self.running_jobs[host] if job.returned() ]
                self.running_jobs[host] = [ job for job in self.running_jobs[host] if job not in _returned ]
                returned.extend(_returned)
                self.returned_jobs.extend(_returned)

            for comm in self.communicators:
                comm.jobs_returned(self, returned)
            
            #
            # Start new jobs
            #


            started = []
            for job in reversed(list(reversed(self.pending_jobs))): # need reversed to remove while iterating
                try:
                    host = self.fitting_host(job)
                    if host:
                        self.handler.start_job(self, job, host)
                        self.running_jobs[host].append(job)
                        started.append(job)

                except HostUnavailableError:
                    self.pending_jobs.remove(job)
                    self.failed_jobs.append(job)
                    for comm in self.communicators:
                        comm.host_unavailable(self, job)

                except StartingError as error:
                    self.pending_jobs.remove(job)
                    self.failed_jobs.append(job)
                    for comm in self.communicators:
                        comm.starting_failed(self, job, error)


            self.pending_jobs = [ job for job in self.pending_jobs if not job.started() ]

            for comm in self.communicators:
                comm.jobs_started(self, started)

            #
            # Check for updatet status information
            #

            for host in self.hosts:
                for job in self.running_jobs[host]:
                    percs, outfiles = job.update_status()

                    for comm in self.communicators:
                        comm.job_updated(self, job, percs, outfiles)


        #
        # Communicate: Handle special communications
        #

        if self.comm:
            for comm in self.communicators:
                comm.communicate(self)


    def register(self, job):
        job.set_id(self.next_id)
        self.next_id += 1

    def free_jobslots(self, host):
        return self.jobslots[host] - len(self.running_jobs[host])
        

    def fitting_host(self, job):

        working_hosts  = [ host for host in self.hosts if self.handler.check_host(host) ]
        possible_hosts = working_hosts if job.hosts == None else [ host for host in working_hosts if host in job.hosts ]

        def key(host): return self.free_jobslots(host)

        try:
            ordered_hosts = sorted(possible_hosts, key = key)
            if len(ordered_hosts) == 0 or self.free_jobslots(ordered_hosts[-1]) == 0:
                return None
            else:
                return ordered_hosts[-1]

        except KeyError:
            raise HostUnavailableError

                  
    def job(self, jobid):
        for job in self.pending_jobs:
            if job.id == jobid: return job

        for job in self.returned_jobs:
            if job.id == jobid: return job

        for job in self.failed_jobs:
            if job.id == jobid: return job

        for job in self.finished_jobs:
            if job.id == jobid: return job

        for host in self.hosts:
            for job in self.running_jobs[host]:
                if job.id == jobid: return job

        return None


    def remove_job(self, job):
        try: self.pending_jobs.remove(job)
        except: pass
        try: self.returned_jobs.remove(job)
        except: pass
        try: self.failed_jobs.remove(job)
        except: pass
        try: self.finished_jobs.remove(job)
        except: pass
        for host in self.hosts:
            try: self.running_jobs[host].remove(job)
            except: pass
