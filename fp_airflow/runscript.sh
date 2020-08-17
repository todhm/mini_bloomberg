#!/usr/bin/env bash

if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user -r /requirements.txt
fi