from ..handler import Handler

from sleekxmpp import ClientXMPP


class XMPPHandlerBase(Handler):

    def __init__(self, jid, passwd, contacts, greeting="Connection established"):
        # also initialize ClientXMPP

        self.xmpp = ClientXMPP(jid, passwd)

        self.jid      = jid
        self.passwd   = passwd
        self.contacts = contacts
        self.greeting = greeting

        self.xmpp.add_event_handler("session_start", self._start_xmpp)
        self.xmpp.add_event_handler("message", self._react)

        self.xmpp.register_plugin('xep_0030') # Service Discovery
        self.xmpp.register_plugin('xep_0199') # XMPP Ping


    def _start_xmpp(self, event):
        self.xmpp.send_presence()
        self.xmpp.get_roster()

        self.send_message(self.greeting)

    # react on messages sent to the xmpp-bot. This makes it possible to
    # control the deamon remotely
    def _react(self, msg):
        pass

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


