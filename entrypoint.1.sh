#!/bin/bash

set -m

exec python chatbot.py &
exec python app.py 