import ssl
import smtplib
from django.core.mail.backends.smtp import EmailBackend as DjangoEmailBackend

class EmailBackend(DjangoEmailBackend):
    def open(self):
        if self.connection is None:
            self.connection = self.connection_class(self.host, self.port)
            if self.use_tls:
                self.connection.starttls(context=ssl._create_unverified_context())
            if self.username and self.password:
                self.connection.login(self.username, self.password)
        return True
