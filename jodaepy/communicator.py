
import sys
from sleekxmpp import ClientXMPP


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

    def close(): pass



class StreamCommunicator(Communicator):
    
    def __init__(self, stream = sys.stdout):
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







class XMPPCommunicator(Communicator):

    def __del__(self):
        self.disconnect()

    def __init__(self, jid, passwd, contacts, accept_from=None, greeting="XMPP communication established", verbosity = 2, answer=None):
        # also initialize ClientXMPP

        self.xmpp = ClientXMPP(jid, passwd)

        self.jid      = jid
        self.passwd   = passwd
        self.contacts = contacts
        self.greeting = greeting
        self.accept_from = accept_from
        self.verbosity = verbosity

        self.messages = []

        self.xmpp.add_event_handler("session_start", self._start_xmpp)
        self.xmpp.add_event_handler("message", self._buffer_message)


        self.xmpp.register_plugin('xep_0030') # Service Discovery
        self.xmpp.register_plugin('xep_0199') # XMPP Ping

        if answer == None:
            self.answer = lambda daemon, comm, msg: None
        else:
            self.answer = answer


    def prepare(self, daemon):
        self.connect()
        self.process()

    def _start_xmpp(self, event):
        self.xmpp.send_presence()
        self.xmpp.get_roster()

        print "[jodaepy] " + self.greeting

        self.send_message(self.greeting)

    def _buffer_message(self, msg):
        if msg['type'] in ['normal', 'chat']:
            if self.accept_from == None or msg['from'] in self.accept_from:
                self.messages.append(msg['body'])

    def connect(self):
        return self.xmpp.connect()

    def disconnect(self):
        return self.xmpp.disconnect()

    def process(self):
        return self.xmpp.process(block=False)

    def send_message(self, mbody, contacts = None):
        contacts = self.contacts if not contacts else contacts
        for contact in contacts:
            self.xmpp.send_message( mto=contact,
                                    mbody=mbody,
                                    mtype='chat' )



    def registration_failed(self, daemon, error):
        if self.verbosity > 1:
            self.send_message("Registration Failed: " + str(error) + "\n")

    def finalization_failed(self, daemon, job, error):
        if self.verbosity > 1:
            self.send_message("Finalization Failed (job %d): " % job.id + str(error) + "\n")

    def starting_failed(self, daemon, job, error):
        if self.verbosity > 1:
            self.send_message("Could not start job (job %d): " % job.id + str(error) + "\n")

    def postprocessing_failed(self, daemon, error):
        if self.verbosity > 1:
            self.send_message("Postprocessing failed: " + str(error) + "\n")

    def workspace_updated(self, deamon, files):
        if self.verbosity > 1:
            if files:
                self.send_message("Found files  " + " ".join(files) + "\n")

    def scripts_updated(self, deamon, scripts):
        if self.verbosity > 1:
            if scripts:
                self.send_message("Found scripts  " + " ".join(scripts) + "\n")

    
    def jobs_registered(self, daemon, jobs):
        if self.verbosity > 1:
            if jobs:
                msg = []
                msg.append("Found jobs\n")
                for job in jobs:
                    msg.append("          %5d '%s'\n" % (job.id, job.title) )
                self.send_message(''.join(msg))


    def jobs_started(self, daemon, jobs):
        if self.verbosity > 1:
            if jobs:
                msg = []
                msg.append("Started jobs\n")
                for job in jobs:
                    msg.append("          %5d, on '%s'\n" % (job.id, job.running_host) )
                self.send_message(''.join(msg))

    def jobs_returned(self, daemon, jobs):
        pass

    def jobs_finalized(self, daemon, jobs):
        if self.verbosity > 1:
            if jobs:
                msg = []
                msg.append("Finalized jobs\n")
                for job in jobs:
                    msg.append("          %5d (%s), %s -- %s\n" % \
                              (job.id, "fail" if job.failed() else "sucess", job.stime, job.ftime) )
                self.send_message(''.join(msg))


    def postprocessing_done(self, daemon, pp):
        if self.verbosity > 1:
            if pp:
                self.send_message("Postprocessing done\n")

    def host_unavailable(self, daemon, job):
        if self.verbosity > 1:
            self.send_message("Expected hosts %s for job %d are unavailable. Dropped this job\n" % (str(job.hosts), job.id) )


    def job_updated(self, daemon, job, dpercs, doutfiles):
        
        perc_old, perc = dpercs
        outfiles_old, outfiles= doutfiles

        if self.verbosity < 3:
            return

        if self.verbosity == 3:
            if perc_old < 25 and perc > 25 or \
               perc_old < 50 and perc > 50 or \
               perc_old < 75 and perc > 75:
                self.send_message("Job %d: %d%% completed\n" % (job.id, perc) )


    def communicate(self, daemon):
        for message in self.messages:
            self.answer(daemon, self, message)
        self.messages = []


    def close():
        self.disconnect()
