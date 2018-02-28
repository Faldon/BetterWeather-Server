import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("BETTERWEATHER_SETTINGS", "production.py")
activate_this = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).__str__() + '/venv/bin/activate_this.py'

with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

from betterweather import app as application
