import sys

#import sentry_sdk
#from sentry_sdk.integrations.wsgi import SentryWsgiMiddleware

sys.path.insert(0, "/var/www/smc")
from run import app as application

#sentry_sdk.init(dsn="https://b20f2406b2974bafb82591948b87523f@sentry.io/1386837")

#application = SentryWsgiMiddleware(application)
