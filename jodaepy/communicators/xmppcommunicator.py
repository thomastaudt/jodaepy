from ..communicator import Communicator

from ..deps.sleekxmpp import ClientXMPP


class XMPPCommunicatorBase(Communicator):

    def __init__(self, jid, passwd, contacts, accept_from=None, greeting="Connection established"):
        # also initialize ClientXMPP

        self.xmpp = ClientXMPP(jid, passwd)

        self.jid      = jid
        self.passwd   = passwd
        self.contacts = contacts
        self.greeting = greeting
        self.accept_from = accept_from

        self.messages = []

        self.xmpp.add_event_handler("session_start", self._start_xmpp)
        self.xmpp.add_event_handler("message", self._buffer_message)


        self.xmpp.register_plugin('xep_0030') # Service Discovery
        self.xmpp.register_plugin('xep_0199') # XMPP Ping


    def _start_xmpp(self, event):
        self.xmpp.send_presence()
        self.xmpp.get_roster()

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
        return self.xmpp.process()

    def send_message(self, mbody, contacts = None):
        contacts = self.contacts if not contacts else contacts
        for contact in contacts:
            self.xmpp.send_message( mto=contact,
                                    mbody=mbody,
                                    mtype='chat' )


