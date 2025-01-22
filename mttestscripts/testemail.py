#To test that the email is working

from syslogdiag.emailing import send_mail_msg

send_mail_msg("This is a test email", "Test email")